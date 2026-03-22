#!/usr/bin/env python3
"""
Incrementally mirror Mesonet station photos to S3.

Linear pipeline:
  1. Load manifest from S3
  2. Crawl source server and compare against manifest
  3. Download new raw JPEGs  →  cache/photos_raw/{station}/{YYYY-MM-DDTHH0000}_{dir}.jpg
  4. Convert to WebP via cwebp  →  cache/photos_web/{station}/{YYYY-MM-DDTHH0000}_{dir}.webp
  5. Upload both to s3://mco-mesonet/
  6. Update manifest at s3://mco-mesonet/photos/manifest.parquet

AWS auth: --profile mco (local) or AWS_ACCESS_KEY_ID env var (CI/CD)
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from io import BytesIO
from pathlib import Path

import boto3
import botocore.exceptions
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_BASE   = "https://data.climate.umt.edu/mesonet/photos"
S3_BUCKET     = "mco-mesonet"
S3_PREFIX_RAW = "photos/raw"
S3_PREFIX_WEB = "photos/web"
MANIFEST_KEY  = "photos/manifest.parquet"
SKIP_LIST_KEY = "photos/skip_list.json"
LOCAL_RAW        = Path("cache/photos_raw")
LOCAL_WEB        = Path("cache/photos_web")
CRAWL_CACHE_FILE = Path("cache/crawl_cache.json")
WEBP_QUALITY  = 75
WEBP_METHOD   = 6
WEBP_WIDTH    = 320

MANIFEST_SCHEMA = pa.schema([
    pa.field("station",     pa.string()),
    pa.field("direction",   pa.string()),
    pa.field("datetime",    pa.string()),   # YYYY-MM-DDTHH0000
    pa.field("s3_key_raw",  pa.string()),   # photos/raw/{station}/{datetime}_{dir}.jpg
    pa.field("s3_key_webp", pa.string()),   # photos/web/{station}/{datetime}_{dir}.webp
])

_IMMUTABLE   = {"CacheControl": "public, max-age=31536000, immutable"}
_SKIP_HREFS  = ("?", "/", "../", "http", "latest")
_FILENAME_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})(\d{2})\d{4}_([A-Za-z]+)\.jpe?g$", re.IGNORECASE)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def crawl(session: requests.Session, url: str) -> list[str]:
    """Return bare entry names from a Caddy directory listing."""
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return [
        a["href"].lstrip("./").rstrip("/")
        for a in BeautifulSoup(r.text, "html.parser").find_all("a", href=True)
        if not any(a["href"].startswith(p) for p in _SKIP_HREFS)
    ]

def parse_filename(filename: str) -> tuple[str, str] | None:
    """
    Parse 'YYYYMMDDHHMMSS_DIR.jpg' into (iso_datetime, direction).
      iso_datetime — 'YYYY-MM-DDTHH0000'  (minutes + seconds zeroed)
      direction    — uppercase, e.g. 'N', 'SNOW', 'NS'
    Returns None if the filename doesn't match.
    """
    m = _FILENAME_RE.match(filename)
    if not m:
        return None
    iso_dt    = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}0000"
    direction = m.group(5).upper()
    return iso_dt, direction

def s3_key(station: str, iso_dt: str, direction: str, ext: str) -> str:
    prefix = S3_PREFIX_RAW if ext == "jpg" else S3_PREFIX_WEB
    return f"{prefix}/{station}/{iso_dt}_{direction}.{ext}"

# ── Manifest ──────────────────────────────────────────────────────────────────

def load_manifest(s3) -> tuple[pd.DataFrame, set[str]]:
    buf = BytesIO()
    try:
        s3.download_fileobj(S3_BUCKET, MANIFEST_KEY, buf)
        buf.seek(0)
        df = pd.read_parquet(buf)
        log.info(f"Loaded manifest: {len(df):,} entries")
        return df, set(df["s3_key_webp"])
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            log.info("No manifest found — starting fresh")
            return pd.DataFrame(columns=MANIFEST_SCHEMA.names), set()
        raise

def load_skip_list(s3) -> set[str]:
    try:
        buf = BytesIO()
        s3.download_fileobj(S3_BUCKET, SKIP_LIST_KEY, buf)
        data = json.loads(buf.getvalue().decode())
        log.info(f"Loaded skip list: {len(data):,} entries")
        return set(data)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return set()
        raise

def save_skip_list(s3, skip_set: set[str]) -> None:
    body = json.dumps(sorted(skip_set)).encode()
    s3.put_object(Bucket=S3_BUCKET, Key=SKIP_LIST_KEY,
                  Body=body, ContentType="application/json")
    log.info(f"Skip list saved ({len(skip_set):,} entries) → s3://{S3_BUCKET}/{SKIP_LIST_KEY}")

def save_manifest(s3, df: pd.DataFrame) -> None:
    buf = BytesIO()
    pq.write_table(
        pa.Table.from_pandas(df, schema=MANIFEST_SCHEMA, preserve_index=False),
        buf, compression="snappy",
    )
    buf.seek(0)
    s3.upload_fileobj(buf, S3_BUCKET, MANIFEST_KEY,
                      ExtraArgs={"ContentType": "application/octet-stream"})
    log.info(f"Manifest saved ({len(df):,} entries) → s3://{S3_BUCKET}/{MANIFEST_KEY}")

# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="mco",
                   help="AWS profile (default: %(default)s; ignored if AWS_ACCESS_KEY_ID is set)")
    p.add_argument("--dry-run", action="store_true",
                   help="List new photos without downloading or uploading")
    p.add_argument("--fresh-crawl", action="store_true",
                   help=f"Re-crawl source server even if {CRAWL_CACHE_FILE} exists")
    return p.parse_args()

def main() -> None:
    args = parse_args()

    # AWS session
    boto_session = (boto3.Session() if os.environ.get("AWS_ACCESS_KEY_ID")
                    else boto3.Session(profile_name=args.profile))
    s3 = boto_session.client("s3")

    # HTTP session
    http = requests.Session()
    http.headers["User-Agent"] = "mesonet-photo-mirror/1.0"
    http.mount("https://", requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(total=3, backoff_factor=0.5),
    ))

    # ── Step 1: Load manifest ─────────────────────────────────────────────────
    log.info("=== Step 1: Load manifest ===")
    manifest_df, existing = load_manifest(s3)
    skip_set = load_skip_list(s3)

    # ── Step 2: Crawl source and compare ─────────────────────────────────────
    log.info("=== Step 2: Crawl source and compare ===")
    if not args.fresh_crawl and CRAWL_CACHE_FILE.exists():
        log.info(f"  Loading crawl cache from {CRAWL_CACHE_FILE}")
        station_files: dict[str, list[str]] = json.loads(CRAWL_CACHE_FILE.read_text())
    else:
        stations = [s for s in crawl(http, f"{SOURCE_BASE}/") if s and "." not in s]
        log.info(f"  Found {len(stations)} stations — crawling file lists…")
        station_files = {}
        for station in stations:
            station_files[station] = crawl(http, f"{SOURCE_BASE}/{station}/")
        CRAWL_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CRAWL_CACHE_FILE.write_text(json.dumps(station_files))
        log.info(f"  Crawl cache saved to {CRAWL_CACHE_FILE}")

    tasks: list[tuple] = []   # (station, filename, iso_dt, direction)
    seen:  set[str]    = set()
    for station, filenames in station_files.items():
        for filename in filenames:
            parsed = parse_filename(filename)
            if not parsed:
                continue
            iso_dt, direction = parsed
            key = s3_key(station, iso_dt, direction, "webp")
            if key not in seen and key not in existing and key not in skip_set:
                seen.add(key)
                tasks.append((station, filename, iso_dt, direction))

    log.info(f"{len(tasks):,} new photos to process  ({len(existing):,} in manifest, {len(skip_set):,} skipped)")

    if args.dry_run or not tasks:
        log.info("Dry run — exiting." if args.dry_run else "Nothing to do.")
        return

    # ── Step 3: Download ──────────────────────────────────────────────────────
    log.info("=== Step 3: Download raw JPEGs ===")
    download_ok:     list[tuple] = []
    download_failed: list[tuple] = []
    new_skips:       set[str]    = set()

    for station, filename, iso_dt, direction in tasks:
        raw = LOCAL_RAW / station / f"{iso_dt}_{direction}.jpg"
        url = f"{SOURCE_BASE}/{station}/{filename}"
        if raw.exists():
            log.info(f"  cached   {raw}")
            download_ok.append((station, filename, iso_dt, direction))
            continue
        log.info(f"  GET      {url}")
        try:
            r = http.get(url, timeout=60)
            r.raise_for_status()
            if not r.content.startswith(b"\xff\xd8"):
                raise ValueError(f"not a JPEG ({len(r.content)} bytes)")
            if not r.content.endswith(b"\xff\xd9"):
                raise ValueError(f"truncated JPEG — no EOI marker ({len(r.content)} bytes)")
            raw.parent.mkdir(parents=True, exist_ok=True)
            raw.write_bytes(r.content)
            log.info(f"  saved    {raw} ({len(r.content):,} bytes)")
            download_ok.append((station, filename, iso_dt, direction))
        except ValueError as exc:
            # Deterministic bad file — skip permanently
            key = s3_key(station, iso_dt, direction, "webp")
            log.warning(f"  SKIP     {station}/{filename}: {exc}")
            new_skips.add(key)
            download_failed.append((station, filename, iso_dt, direction))
        except Exception as exc:
            # Transient error — allow retry next run
            log.warning(f"  FAILED   {station}/{filename}: {exc}")
            download_failed.append((station, filename, iso_dt, direction))

    log.info(f"Downloads: {len(download_ok):,} ok, {len(new_skips):,} permanently skipped, {len(download_failed) - len(new_skips):,} transient failures")

    # ── Step 4: Convert to WebP ───────────────────────────────────────────────
    log.info("=== Step 4: Convert to WebP ===")
    convert_ok:     list[tuple] = []
    convert_failed: list[tuple] = []

    for station, filename, iso_dt, direction in download_ok:
        raw = LOCAL_RAW / station / f"{iso_dt}_{direction}.jpg"
        web = LOCAL_WEB / station / f"{iso_dt}_{direction}.webp"
        if web.exists():
            log.info(f"  cached   {web}")
            convert_ok.append((station, filename, iso_dt, direction))
            continue
        log.info(f"  cwebp    {raw} → {web}")
        web.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["cwebp", "-q", str(WEBP_QUALITY), "-m", str(WEBP_METHOD),
             "-resize", str(WEBP_WIDTH), "0", str(raw), "-o", str(web)],
            capture_output=True,
        )
        if result.returncode != 0:
            diag = subprocess.run(["file", str(raw)], capture_output=True, text=True)
            size = raw.stat().st_size if raw.exists() else "missing"
            head = raw.read_bytes()[:16].hex(" ") if raw.exists() else ""
            log.warning(
                f"  FAILED   {station}/{filename}: cwebp exit {result.returncode}\n"
                f"           stderr:  {result.stderr.decode(errors='replace').strip()}\n"
                f"           file:    {diag.stdout.strip()}\n"
                f"           size:    {size} bytes  head: {head}"
            )
            convert_failed.append((station, filename, iso_dt, direction))
        else:
            convert_ok.append((station, filename, iso_dt, direction))

    log.info(f"Conversions: {len(convert_ok):,} ok, {len(convert_failed):,} failed")

    # ── Step 5: Upload to S3 ──────────────────────────────────────────────────
    log.info("=== Step 5: Upload to S3 ===")
    new_rows:      list[dict]  = []
    upload_failed: list[tuple] = []

    for station, filename, iso_dt, direction in convert_ok:
        raw      = LOCAL_RAW / station / f"{iso_dt}_{direction}.jpg"
        web      = LOCAL_WEB / station / f"{iso_dt}_{direction}.webp"
        key_raw  = s3_key(station, iso_dt, direction, "jpg")
        key_webp = s3_key(station, iso_dt, direction, "webp")
        log.info(f"  upload   {station}/{iso_dt}_{direction}")
        try:
            s3.upload_file(str(raw), S3_BUCKET, key_raw,
                           ExtraArgs={"ContentType": "image/jpeg", **_IMMUTABLE})
            s3.upload_file(str(web), S3_BUCKET, key_webp,
                           ExtraArgs={"ContentType": "image/webp", **_IMMUTABLE})
            new_rows.append(dict(station=station, direction=direction, datetime=iso_dt,
                                 s3_key_raw=key_raw, s3_key_webp=key_webp))
        except Exception as exc:
            log.warning(f"  FAILED   {station}/{iso_dt}_{direction}: {exc}")
            upload_failed.append((station, filename, iso_dt, direction))

    log.info(f"Uploads: {len(new_rows):,} ok, {len(upload_failed):,} failed")

    # ── Step 6: Update manifest and skip list ────────────────────────────────
    if new_rows:
        log.info("=== Step 6: Update manifest ===")
        updated = pd.concat([manifest_df, pd.DataFrame(new_rows)],
                            ignore_index=True).drop_duplicates("s3_key_webp")
        save_manifest(s3, updated)

    if new_skips:
        log.info("=== Step 6b: Update skip list ===")
        save_skip_list(s3, skip_set | new_skips)

    transient_failures = len(download_failed) - len(new_skips) + len(convert_failed) + len(upload_failed)
    log.info(f"Done. {len(new_rows):,} uploaded, {len(new_skips):,} added to skip list, {transient_failures:,} transient failures.")
    if transient_failures:
        sys.exit(1)

if __name__ == "__main__":
    main()
