# eTrade Licence Crawler

Parallel crawler for the Ethiopian Ministry of Trade's eTrade portal licence
lookup API:

```
GET https://etrade.gov.et/api/BusinessMain/GetBusinessByLicenseNo?LicenseNo=<licence>&Tin=<tin>&Lang=en
```

For every trade licence in a crawled TIN database (`tin_data.db` from the
[tinitin](../tinitin) project) it fetches the licence-level record and stores
the raw response in a local SQLite database. This is the data the TIN-level
lookup never returned: **address** (region/zone/woreda/kebele/house no),
**phones**, **associate position**, and **licence status** ("Is active",
"Closed", "Not renewed for 2 years...").

## Files

| file | purpose |
|---|---|
| `license_crawler.py` | the crawler: `build` (queue), `run` (parallel fetch), `stats` |
| `compare_company_licences.py` | analysis: is fetching one licence per company enough? (answer: no — see below) |

## Setup

```bash
pip install requests        # only dependency
```

Python 3.9+. The source DB path is configurable; defaults expect
`tin_data.db` next to the working directory (or pass `--source`).

## Usage

```bash
# 1. Fill the work queue (resumable; re-running skips existing jobs)
python license_crawler.py build --mode license --source ../tinitin/tin_data.db

# 2. Crawl (Ctrl+C safe — results are batched through a single writer and
#    every completed request is committed; restart resumes where it stopped)
python license_crawler.py run --workers 12 --delay 0 \
    --source ../tinitin/tin_data.db

# 3. Progress
python license_crawler.py stats
```

Modes: `--mode company` fetches the first licence per TIN (2.53M requests);
`--mode license` fetches every licence (3.47M). **Use `license`** — see
[findings](#findings).

Useful flags for `run`: `--max-requests N` (stop after N — used for the ramp
test), `--retries`, `--timeout`, `--keep-photo`, `--crawl-db`, `--source`.

Output schema (`crawl.db`): a `queue` table (tin, licence, done, attempts)
and a `responses` table (raw JSON, minus photos, plus http status, latency,
error, timestamp).

## API quirks this crawler works around

- **Referer required.** Without a same-site `Referer` the server answers
  HTTP 417 "Invalid referer headerr". We send the referer of the portal's own
  `business-license-checker` page, plus `x-clientid: angular-app` and a full
  browser header set.
- **TLS.** The host's certificate chain does not validate against Python's CA
  bundle, so verification is off (`verify=False`).
- **Photos.** Each associate ships a base64 JPEG (up to ~900KB). Photos are
  98.7% of all response bytes; the crawler strips them before saving
  (`--keep-photo` to disable). Stripped, the average response is ~830 bytes.
- **HTTP 204** means "licence not found" — recorded as `empty`, not failed.

## Findings (measured Oct 2026)

**Server limits.** No throttling or blocking observed up to 64 concurrent
connections (zero 429/403/417/5xx across ~1,300 test requests). Throughput
saturates at **~5 req/s per IP** regardless of worker count; beyond 8–16
workers latency just inflates (p50 450ms → 1.9s, p95 up to 8s) without adding
throughput. Run 8–16 workers and monitor. Full per-licence crawl ≈ 8 days at
that rate; a second crawler on another IP scales roughly linearly.

**Per-company vs per-licence.** Across 100 sampled multi-licence companies,
comparing each company's own licences: associates are identical 96% of the
time, but **AddressInfo matches only 63%** and licence status only 42%.
549,378 of 2,528,839 companies have ≥2 licences, so first-licence-only
fetching would mislabel addresses for ~200K companies. Fetch per licence.

**Scale.** Source data: 1,979,461 single-licence + 549,378 multi-licence
companies; 3,474,406 licence numbers total.

## Related

- [../tinitin](../tinitin) — the parent project: crawled TIN data (SQLite →
  Postgres → API → Next.js directory site). The `demo_license_lookup.py` and
  `license_lookup.db` there are the earlier single-threaded proof of concept
  this crawler replaces.
