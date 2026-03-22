#!/usr/bin/env python3
"""
Incrementally mirror Mesonet station photos to S3.

For each photo on the source server not yet in the manifest:
  1. Download the raw JPEG  →  cache/photos_raw/{station}/{YYYY-MM-DDTHH0000}_{dir}.jpg
  2. Convert to WebP via cwebp  →  cache/photos_web/{station}/{YYYY-MM-DDTHH0000}_{dir}.webp
  3. Upload both to s3://mco-mesonet/photos/{station}/
  4. Record in the manifest at s3://mco-mesonet/photos/manifest.parquet

AWS auth: --profile mco (local) or AWS_ACCESS_KEY_ID env var (CI/CD)
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import boto3
import botocore.exceptions
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────

SOURCE_BASE     = "https://data.climate.umt.edu/mesonet/photos"
S3_BUCKET       = "mco-mesonet"
S3_PREFIX_RAW   = "photos/raw"
S3_PREFIX_WEB   = "photos/web"
MANIFEST_KEY    = "photos/manifest.parquet"
LOCAL_RAW       = Path("cache/photos_raw")
LOCAL_WEB       = Path("cache/photos_web")
WEBP_QUALITY    = 75
WEBP_METHOD     = 6
WEBP_WIDTH      = 320
DEFAULT_WORKERS = 8

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

# ── Crawling ──────────────────────────────────────────────────────────────────

def crawl(session: requests.Session, url: str) -> list[str]:
    """Return bare entry names from a Caddy directory listing."""
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return [
        a["href"].lstrip("./").rstrip("/")
        for a in BeautifulSoup(r.text, "html.parser").find_all("a", href=True)
        if not any(a["href"].startswith(p) for p in _SKIP_HREFS)
    ]

# ── Filename parsing ──────────────────────────────────────────────────────────

def parse_filename(filename: str) -> tuple[str, str] | None:
    """
    Parse 'YYYYMMDDHHMMSS_DIR.jpg' into (iso_datetime, direction).
      iso_datetime — 'YYYY-MM-DDTHH0000'  (minutes + seconds zeroed)
      direction    — lowercase, e.g. 'n', 'snow', 'ns'
    Returns None if the filename doesn't match.
    """
    m = _FILENAME_RE.match(filename)
    if not m:
        return None
    iso_dt    = f"{m.group(1)}-{m.group(2)}-{m.group(3)}T{m.group(4)}0000"
    direction = m.group(5).upper()
    return iso_dt, direction

# ── S3 keys ───────────────────────────────────────────────────────────────────

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

# ── Processing ────────────────────────────────────────────────────────────────

def process(session: requests.Session, s3, station: str, filename: str,
            iso_dt: str, direction: str) -> dict:
    raw      = LOCAL_RAW / station / f"{iso_dt}_{direction}.jpg"
    web      = LOCAL_WEB / station / f"{iso_dt}_{direction}.webp"
    key_raw  = s3_key(station, iso_dt, direction, "jpg")
    key_webp = s3_key(station, iso_dt, direction, "webp")

    # Download raw JPEG
    if not raw.exists():
        r = session.get(f"{SOURCE_BASE}/{station}/{filename}", timeout=60)
        r.raise_for_status()
        raw.parent.mkdir(parents=True, exist_ok=True)
        raw.write_bytes(r.content)

    # Convert to WebP
    if not web.exists():
        web.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["cwebp", "-q", str(WEBP_QUALITY), "-m", str(WEBP_METHOD),
             "-resize", str(WEBP_WIDTH), "0", str(raw), "-o", str(web)],
            check=True, capture_output=True,
        )

    # Upload both
    s3.upload_file(str(raw), S3_BUCKET, key_raw,
                   ExtraArgs={"ContentType": "image/jpeg", **_IMMUTABLE})
    s3.upload_file(str(web), S3_BUCKET, key_webp,
                   ExtraArgs={"ContentType": "image/webp", **_IMMUTABLE})

    return dict(station=station, direction=direction, datetime=iso_dt,
                s3_key_raw=key_raw, s3_key_webp=key_webp)

# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                   help="Parallel workers (default: %(default)s)")
    p.add_argument("--profile", default="mco",
                   help="AWS profile (default: %(default)s; ignored if AWS_ACCESS_KEY_ID is set)")
    p.add_argument("--dry-run", action="store_true",
                   help="Count new photos without downloading or uploading")
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
        pool_connections=args.workers,
        pool_maxsize=args.workers * 2,
        max_retries=requests.adapters.Retry(total=3, backoff_factor=0.5),
    ))

    # Load manifest
    manifest_df, existing = load_manifest(s3)

    # Enumerate source
    log.info("Enumerating stations…")
    stations = [s for s in crawl(http, f"{SOURCE_BASE}/") if "." not in s]
    log.info(f"Found {len(stations)} stations")

    # Build task list — one task per new station/direction/hour slot
    tasks: list[tuple] = []
    seen: set[str] = set()
    for station in stations:
        for filename in crawl(http, f"{SOURCE_BASE}/{station}/"):
            parsed = parse_filename(filename)
            if not parsed:
                continue
            iso_dt, direction = parsed
            key = s3_key(station, iso_dt, direction, "webp")
            if key not in seen and key not in existing:
                seen.add(key)
                tasks.append((station, filename, iso_dt, direction))

    log.info(f"{len(tasks):,} new photos  ({len(existing):,} already in manifest)")

    if args.dry_run:
        log.info("Dry run — exiting.")
        return
    if not tasks:
        log.info("Nothing to do.")
        return

    # Process in parallel
    new_rows: list[dict] = []
    errors:   list[tuple] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process, http, s3, *t): t for t in tasks}
        with tqdm(total=len(tasks), unit="photo") as bar:
            for fut in as_completed(futures):
                bar.update(1)
                try:
                    new_rows.append(fut.result())
                except Exception as exc:
                    t = futures[fut]
                    errors.append(t)
                    log.warning(f"FAILED {t[0]}/{t[1]}: {exc}")

    # Save manifest
    if new_rows:
        updated = pd.concat([manifest_df, pd.DataFrame(new_rows)],
                            ignore_index=True).drop_duplicates("s3_key_webp")
        save_manifest(s3, updated)

    log.info(f"Done. {len(new_rows):,} uploaded" + (f", {len(errors):,} failed." if errors else "."))
    if errors:
        sys.exit(1)

if __name__ == "__main__":
    main()
