#!/usr/bin/env python3
"""
For a sample of multi-licence companies, fetch ALL their licences from the
eTrade licence API and check whether the company-level data (address,
associates, capital, ...) is identical across licences.

If it is, fetching one licence per company is enough and the full crawl
shrinks from ~3.47M to ~2.53M requests.

Usage: python compare_company_licences.py [--sample 25] [--workers 8]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

from license_crawler import fetch_license, new_session

SOURCE_DB = "tin_data.db"


def sample_multi_licence_companies(n: int):
    sq = sqlite3.connect(f"file:{SOURCE_DB}?mode=ro", uri=True)
    found = 0
    for tin, raw in sq.execute(
        "SELECT tin_number, response FROM tin_responses "
        "WHERE status = 'completed' AND tin_number != '' ORDER BY id"
    ):
        try:
            rec = json.loads(raw)
        except (TypeError, ValueError):
            continue
        licences = [
            b.get("LicenceNumber")
            for b in (rec.get("Businesses") or [])
            if isinstance(b, dict) and b.get("LicenceNumber")
        ]
        if 2 <= len(licences) <= 6:
            yield tin, licences
            found += 1
            if found >= n:
                return


def norm(obj):
    """JSON-normalize for equality checks (sort dict keys)."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--sample", type=int, default=25)
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()

    companies = list(sample_multi_licence_companies(args.sample))
    jobs = [(tin, lic) for tin, licences in companies for lic in licences]
    print(f"sample: {len(companies)} companies, {len(jobs)} licences to fetch")

    session = new_session()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        # fetch_license is not thread-safe per session? It is: requests.Session
        # is thread-safe for simple GETs, but to be safe give each job its own
        # lookup through a shared pool of sessions via the executor.
        results = dict(
            zip(jobs, pool.map(lambda j: fetch_license(session, j[0], j[1]), jobs))
        )

    agg = {
        "address_same": 0,
        "associates_same": 0,
        "capital_same": 0,
        "subgroups_same": 0,
        "status_same": 0,
        "everything_same": 0,
        "fetch_failed": 0,
    }
    print("\nper-company detail (differing fields in <>)")
    for tin, licences in companies:
        recs = []
        for lic in licences:
            http, status, body, error, _lat = results[(tin, lic)]
            if status != "completed":
                recs.append(None)
                continue
            recs.append(json.loads(body))

        if not recs or any(r is None for r in recs):
            agg["fetch_failed"] += 1
            print(f"  {tin}: fetch problem, skipped")
            continue

        diffs = []
        addr = {norm(r.get("AddressInfo")) for r in recs}
        assoc = {norm(r.get("AssociateShortInfos")) for r in recs}
        cap = {norm(r.get("Capital")) for r in recs}
        subs = {norm(r.get("SubGroups")) for r in recs}
        stat = {norm(r.get("StatusDescription")) for r in recs}
        if len(addr) > 1:
            diffs.append("AddressInfo")
        if len(assoc) > 1:
            diffs.append("Associates")
        if len(cap) > 1:
            diffs.append("Capital")
        if len(subs) > 1:
            diffs.append("SubGroups")
        if len(stat) > 1:
            diffs.append("Status")

        same_addr, same_assoc = len(addr) == 1, len(assoc) == 1
        everything = not diffs
        agg["address_same"] += same_addr
        agg["associates_same"] += same_assoc
        agg["capital_same"] += len(cap) == 1
        agg["subgroups_same"] += len(subs) == 1
        agg["status_same"] += len(stat) == 1
        agg["everything_same"] += everything

        flag = "IDENTICAL" if everything else "differs: " + ",".join(diffs)
        print(f"  {tin} ({len(licences)} licences): {flag}")

    n = len(companies)
    print(f"\nsummary over {n} companies:")
    for k, v in agg.items():
        print(f"  {k}: {v}/{n} ({v / n * 100:.0f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
