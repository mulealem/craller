#!/usr/bin/env python3
"""
Parallel crawler for the eTrade licence lookup API.

  https://etrade.gov.et/api/BusinessMain/GetBusinessByLicenseNo?LicenseNo=..&Tin=..&Lang=en

Subcommands:
  build   Scan tin_data.db and fill the work queue in the crawl DB.
          --mode company  one job per company (first licence)   [default]
          --mode license  one job per licence number
  run     Crawl the queue with N parallel workers. Resumable: completed jobs
          are skipped, in-flight work is re-fetched on restart.
  stats   Print queue/response counts.

Examples:
  python license_crawler.py build --mode company
  python license_crawler.py run --workers 8 --delay 0.2
  python license_crawler.py run --workers 16 --delay 0 --max-requests 200   # ramp test
  python license_crawler.py stats

Design notes:
  * Photos (base64 JPEGs, ~99% of response bytes) are stripped before saving.
  * Work is partitioned across workers by queue.id % workers, so workers never
    contend for the same job and a restart resumes cleanly.
  * Results go through a single writer thread (SQLite likes one writer), batched.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests
import urllib3

SOURCE_DB = "tin_data.db"
CRAWL_DB = "crawl.db"
API_URL = "https://etrade.gov.et/api/BusinessMain/GetBusinessByLicenseNo"
TIMEOUT = 20

# etrade.gov.et serves a TLS chain Python's CA bundle cannot verify; the API
# also rejects requests without a same-site Referer (HTTP 417). See fetch().
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BROWSER_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "dnt": "1",
    "priority": "u=1, i",
    "sec-ch-ua": '"Chromium";v="154", "Microsoft Edge";v="154", "Not A(Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36 Edg/154.0.0.0",
    "x-clientid": "angular-app",
}


# ---------------------------------------------------------------------------
# Fetch primitive (shared by the comparison script)
# ---------------------------------------------------------------------------
def strip_photo(body: str) -> str:
    """Remove base64 associate photos from a response body."""
    try:
        rec = json.loads(body)
    except ValueError:
        return body

    def strip(obj):
        if isinstance(obj, dict):
            obj.pop("Photo", None)
            for v in obj.values():
                strip(v)
        elif isinstance(obj, list):
            for v in obj:
                strip(v)
        return obj

    return json.dumps(strip(rec), ensure_ascii=False, separators=(",", ":"))


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    return s


def fetch_license(
    session: requests.Session,
    tin: str,
    licence: str,
    keep_photo: bool = False,
    timeout: int = TIMEOUT,
) -> tuple[int | None, str, str | None, str | None, float]:
    """Returns (http_status, status, body, error, latency_seconds)."""
    url = (
        f"{API_URL}?LicenseNo={quote(licence, safe='')}"
        f"&Tin={quote(tin, safe='')}&Lang=en"
    )
    # The real frontend hits the API from the business-license-checker page;
    # without a same-site referer the server answers 417 "Invalid referer".
    referer = (
        "https://etrade.gov.et/business-license-checker"
        f"?licenseNo={quote(licence, safe='')}&tin={tin}"
    )
    started = time.monotonic()
    try:
        r = session.get(url, timeout=timeout, verify=False, headers={"Referer": referer})
    except requests.RequestException as exc:
        return None, "failed", None, f"{type(exc).__name__}: {exc}", time.monotonic() - started

    latency = time.monotonic() - started
    body = r.text
    if r.status_code in (200, 204) and not body.strip():
        # 204 No Content = licence not found upstream
        return r.status_code, "empty", None, None, latency
    if r.status_code == 200:
        if not keep_photo:
            body = strip_photo(body)
        return 200, "completed", body, None, latency
    return r.status_code, "failed", (body[:4000] if body else None), f"HTTP {r.status_code}", latency


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id       INTEGER PRIMARY KEY,
    tin      TEXT NOT NULL,
    licence  TEXT NOT NULL,
    done     INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS queue_pair_idx ON queue (tin, licence);
CREATE INDEX IF NOT EXISTS queue_done_idx ON queue (done, id);

CREATE TABLE IF NOT EXISTS responses (
    queue_id    INTEGER PRIMARY KEY,
    tin         TEXT NOT NULL,
    licence     TEXT NOT NULL,
    http_status INTEGER,
    status      TEXT NOT NULL,
    response    TEXT,
    error       TEXT,
    latency_ms  INTEGER,
    fetched_at  TEXT NOT NULL
);
"""


def connect(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(SCHEMA)
    return con


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
def cmd_build(args: argparse.Namespace) -> int:
    if not os.path.exists(args.source):
        print(f"source DB not found: {args.source}", file=sys.stderr)
        return 1

    sq = sqlite3.connect(f"file:{args.source}?mode=ro", uri=True)
    out = connect(args.crawl_db)

    existing = {r[0] for r in out.execute("SELECT tin || '|' || licence FROM queue")}
    print(f"queue already holds {len(existing):,} jobs")

    batch: list[tuple[str, str]] = []
    seen_this_run: set[str] = set()
    inserted = skipped = 0

    def flush():
        nonlocal inserted
        rows = []
        for tin, lic in batch:
            key = f"{tin}|{lic}"
            if key in existing or key in seen_this_run:
                continue
            seen_this_run.add(key)
            rows.append((tin, lic))
        if rows:
            out.executemany("INSERT OR IGNORE INTO queue (tin, licence) VALUES (?, ?)", rows)
            out.commit()
            inserted += len(rows)
        batch.clear()

    started = time.time()
    cur = sq.execute(
        "SELECT tin_number, response FROM tin_responses "
        "WHERE status = 'completed' AND tin_number != '' ORDER BY id"
    )
    scanned = 0
    for tin, raw in cur:
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError):
            continue
        licences = [
            b.get("LicenceNumber")
            for b in (rec.get("Businesses") or [])
            if isinstance(b, dict) and b.get("LicenceNumber")
        ]
        if not licences:
            continue
        if args.mode == "company":
            batch.append((tin, licences[0]))
        else:
            batch.extend((tin, lic) for lic in licences)
        scanned += 1
        if len(batch) >= 5000:
            flush()
            if scanned % 100_000 == 0:
                rate = scanned / max(time.time() - started, 1e-9)
                print(f"  scanned {scanned:,} companies, +{inserted:,} jobs ({rate:.0f}/s)", flush=True)
    flush()
    total = out.execute("SELECT COUNT(*), SUM(done) FROM queue").fetchone()
    print(
        f"done: scanned {scanned:,} companies, queue now {total[0]:,} jobs "
        f"({total[1] or 0:,} already completed), mode={args.mode}, "
        f"took {time.time() - started:.0f}s"
    )
    sq.close()
    out.close()
    return 0


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------
class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.ok = 0
        self.empty = 0
        self.failed = 0
        self.retries = 0
        self.by_http: dict[str, int] = {}
        self.latencies: list[float] = []
        self.fetched = 0  # requests that produced a final result

    def add(self, status: str, http: int | None, error: str | None, latency: float) -> None:
        with self.lock:
            self.fetched += 1
            self.latencies.append(latency)
            key = str(http) if http is not None else "net"
            self.by_http[key] = self.by_http.get(key, 0) + 1
            if status == "completed":
                self.ok += 1
            elif status == "empty":
                self.empty += 1
            else:
                self.failed += 1

    def retry(self) -> None:
        with self.lock:
            self.retries += 1

    def snapshot(self) -> dict:
        with self.lock:
            lat = sorted(self.latencies)
            n = len(lat)
            return {
                "fetched": self.fetched,
                "ok": self.ok,
                "empty": self.empty,
                "failed": self.failed,
                "retries": self.retries,
                "http": dict(self.by_http),
                "p50_ms": int(lat[n // 2] * 1000) if n else 0,
                "p95_ms": int(lat[int(n * 0.95)] * 1000) if n else 0,
            }


def worker(
    wid: int,
    total_workers: int,
    args: argparse.Namespace,
    stats: Stats,
    results: "queue.Queue[tuple]",
    stop: threading.Event,
) -> None:
    """Crawl queue rows where id % total_workers == wid."""
    con = sqlite3.connect(f"file:{args.crawl_db}?mode=ro", uri=True)
    session = new_session()
    last_id = 0
    fetched_here = 0

    while not stop.is_set():
        if args.max_requests and stats.fetched >= args.max_requests:
            break
        rows = con.execute(
            "SELECT id, tin, licence FROM queue "
            "WHERE done = 0 AND attempts < ? AND id % ? = ? AND id > ? "
            "ORDER BY id LIMIT ?",
            (args.retries + 1, total_workers, wid, last_id, 200),
        ).fetchall()
        if not rows:
            break  # this partition exhausted

        for qid, tin, lic in rows:
            if stop.is_set() or (args.max_requests and stats.fetched >= args.max_requests):
                break
            last_id = qid

            attempts = 0
            while True:
                http, status, body, error, latency = fetch_license(
                    session, tin, lic, keep_photo=args.keep_photo, timeout=args.timeout
                )
                retriable = http is None or (isinstance(http, int) and http >= 500)
                throttled = http in (429, 403, 417)
                if retriable or throttled:
                    if attempts < args.retries:
                        attempts += 1
                        stats.retry()
                        time.sleep(min(2 ** attempts, 10))
                        continue
                break

            results.put(
                (
                    qid, tin, lic, http, status, body, error,
                    int(latency * 1000),
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    attempts,
                )
            )
            stats.add(status, http, error, latency)
            fetched_here += 1
            if args.delay:
                time.sleep(args.delay)

    con.close()


def writer(results: "queue.Queue[tuple]", crawl_db: str, stop: threading.Event, done: threading.Event) -> None:
    con = connect(crawl_db)
    batch: list[tuple] = []
    while True:
        try:
            item = results.get(timeout=0.5)
        except queue.Empty:
            if stop.is_set() and results.empty():
                break
            if batch:
                _write(con, batch)
                batch.clear()
            continue
        batch.append(item)
        while len(batch) >= 100:
            _write(con, batch)
            batch.clear()
    if batch:
        _write(con, batch)
    con.close()
    done.set()


def _write(con: sqlite3.Connection, batch: list[tuple]) -> None:
    con.executemany(
        "INSERT OR REPLACE INTO responses "
        "(queue_id, tin, licence, http_status, status, response, error, latency_ms, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [b[:9] for b in batch],
    )
    con.executemany(
        "UPDATE queue SET done = 1, attempts = attempts + 1 + ? WHERE id = ?",
        [(b[9], b[0]) for b in batch],
    )
    con.commit()


def cmd_run(args: argparse.Namespace) -> int:
    con = connect(args.crawl_db)
    total, done = con.execute("SELECT COUNT(*), SUM(done) FROM queue").fetchone()
    con.close()
    if not total:
        print("queue is empty -- run `build` first", file=sys.stderr)
        return 1
    print(
        f"queue: {total:,} jobs ({done or 0:,} done, {total - (done or 0):,} to go) | "
        f"workers={args.workers} delay={args.delay}s"
    )

    stats = Stats()
    results: "queue.Queue[tuple]" = queue.Queue(maxsize=2000)
    stop = threading.Event()
    writer_done = threading.Event()

    wt = threading.Thread(target=writer, args=(results, args.crawl_db, stop, writer_done), daemon=True)
    wt.start()

    threads = [
        threading.Thread(target=worker, args=(i, args.workers, args, stats, results, stop), daemon=True)
        for i in range(args.workers)
    ]
    started = time.time()
    for t in threads:
        t.start()

    last_fetched = 0
    last_time = time.time()
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(10)
            snap = stats.snapshot()
            now = time.time()
            rate = (snap["fetched"] - last_fetched) / max(now - last_time, 1e-9)
            remaining_jobs = total - (done or 0) - snap["fetched"]
            eta_s = remaining_jobs / rate if rate > 0.01 else float("inf")
            eta = f"{eta_s / 3600:.1f}h" if eta_s != float("inf") and eta_s > 7200 else f"{eta_s / 60:.0f}m"
            print(
                f"  [{now - started:7.0f}s] fetched={snap['fetched']:,} ok={snap['ok']:,} "
                f"empty={snap['empty']:,} failed={snap['failed']:,} retries={snap['retries']:,} "
                f"| {rate:.1f} req/s | p50={snap['p50_ms']}ms p95={snap['p95_ms']}ms | eta {eta}",
                flush=True,
            )
            last_fetched, last_time = snap["fetched"], now
            if args.max_requests and snap["fetched"] >= args.max_requests:
                stop.set()
    except KeyboardInterrupt:
        print("\nCtrl+C -- stopping workers, saving results...", file=sys.stderr)
        stop.set()
    for t in threads:
        t.join(timeout=30)
    # drain results and stop the writer
    deadline = time.time() + 60
    while not results.empty() and time.time() < deadline:
        time.sleep(0.2)
    stop.set()
    writer_done.wait(timeout=30)

    snap = stats.snapshot()
    elapsed = time.time() - started
    print(
        f"done in {elapsed:.0f}s: {snap['fetched']:,} fetched, ok={snap['ok']:,} "
        f"empty={snap['empty']:,} failed={snap['failed']:,} | overall "
        f"{snap['fetched'] / max(elapsed, 1e-9):.2f} req/s | http codes: {snap['http']}"
    )
    return 0


# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------
def cmd_stats(args: argparse.Namespace) -> int:
    con = sqlite3.connect(f"file:{args.crawl_db}?mode=ro", uri=True)
    total, done = con.execute("SELECT COUNT(*), SUM(done) FROM queue").fetchone()
    print(f"queue: {total:,} jobs, {done or 0:,} done, {total - (done or 0):,} remaining")
    for row in con.execute(
        "SELECT status, COUNT(*), AVG(latency_ms), AVG(LENGTH(response)) FROM responses GROUP BY status"
    ):
        print(f"  {row[0]}: n={row[1]:,} avg_latency={row[2] or 0:.0f}ms avg_size={row[3] or 0:.0f}B")
    con.close()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="fill the work queue from tin_data.db")
    b.add_argument("--source", default=SOURCE_DB)
    b.add_argument("--crawl-db", default=CRAWL_DB)
    b.add_argument("--mode", choices=["company", "license"], default="company",
                   help="company = first licence per TIN; license = every licence")

    r = sub.add_parser("run", help="crawl the queue with parallel workers")
    r.add_argument("--crawl-db", default=CRAWL_DB)
    r.add_argument("--workers", type=int, default=8)
    r.add_argument("--delay", type=float, default=0.2, help="per-worker delay between requests (s)")
    r.add_argument("--retries", type=int, default=2, help="retries for network/5xx/throttle errors")
    r.add_argument("--timeout", type=int, default=TIMEOUT)
    r.add_argument("--max-requests", type=int, default=0, help="stop after N requests (0 = no limit)")
    r.add_argument("--keep-photo", action="store_true")

    s = sub.add_parser("stats", help="queue/response summary")
    s.add_argument("--crawl-db", default=CRAWL_DB)

    args = p.parse_args()
    return {"build": cmd_build, "run": cmd_run, "stats": cmd_stats}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
