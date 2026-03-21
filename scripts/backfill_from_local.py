#!/usr/bin/env python3
"""
Upload locally backfilled photos to S3 and build the manifest.

Steps:
  1. aws s3 sync cache/photos_raw/ → s3://mco-mesonet/photos/raw/
  2. aws s3 sync cache/photos_web/ → s3://mco-mesonet/photos/web/
  3. Scan cache/photos_web/ to build manifest.parquet
  4. Upload manifest to s3://mco-mesonet/photos/manifest.parquet

Assumes files are already renamed to ISO 8601 format:
  {YYYY-MM-DDTHH0000}_{dir}.jpg / .webp

AWS auth: --profile mco (local) or AWS_ACCESS_KEY_ID env var (CI/CD)
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────

S3_BUCKET     = "mco-mesonet"
S3_PREFIX_RAW = "photos/raw"
S3_PREFIX_WEB = "photos/web"
MANIFEST_KEY  = "photos/manifest.parquet"
LOCAL_RAW     = Path("cache/photos_raw")
LOCAL_WEB     = Path("cache/photos_web")

MANIFEST_SCHEMA = pa.schema([
    pa.field("station",     pa.string()),
    pa.field("direction",   pa.string()),
    pa.field("datetime",    pa.string()),
    pa.field("s3_key_raw",  pa.string()),
    pa.field("s3_key_webp", pa.string()),
])

_WEBP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}0000)_([A-Z]+)\.webp$")
_IMMUTABLE = "public, max-age=31536000, immutable"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────

def run_sync(src: Path, s3_dest: str, content_type: str, include: str,
             profile: str | None, dry_run: bool) -> subprocess.Popen:
    cmd = [
        "aws", "s3", "sync", str(src), s3_dest,
        "--content-type", content_type,
        "--cache-control", _IMMUTABLE,
        "--no-progress",
        "--exclude", "*",
        "--include", include,
    ]
    if dry_run:
        cmd.append("--dryrun")
    if profile:
        cmd += ["--profile", profile]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)


def build_manifest() -> pd.DataFrame:
    """Scan cache/photos_web/ and build manifest rows for every .webp file."""
    webp_files = list(LOCAL_WEB.rglob("*.webp"))
    log.info(f"Building manifest from {len(webp_files):,} WebP files…")
    rows = []
    for webp in tqdm(webp_files, unit="file"):
        m = _WEBP_RE.match(webp.name)
        if not m:
            continue
        station = webp.parent.name
        iso_dt, direction = m.group(1), m.group(2)
        rows.append({
            "station":     station,
            "direction":   direction,
            "datetime":    iso_dt,
            "s3_key_raw":  f"{S3_PREFIX_RAW}/{station}/{iso_dt}_{direction}.jpg",
            "s3_key_webp": f"{S3_PREFIX_WEB}/{station}/{iso_dt}_{direction}.webp",
        })
    log.info(f"Manifest: {len(rows):,} entries")
    return pd.DataFrame(rows, columns=MANIFEST_SCHEMA.names)


def upload_manifest(s3, df: pd.DataFrame) -> None:
    buf = BytesIO()
    pq.write_table(
        pa.Table.from_pandas(df, schema=MANIFEST_SCHEMA, preserve_index=False),
        buf, compression="snappy",
    )
    buf.seek(0)
    s3.upload_fileobj(buf, S3_BUCKET, MANIFEST_KEY,
                      ExtraArgs={"ContentType": "application/octet-stream"})
    log.info(f"Manifest uploaded → s3://{S3_BUCKET}/{MANIFEST_KEY}")

# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="mco",
                   help="AWS profile (default: %(default)s; ignored if AWS_ACCESS_KEY_ID is set)")
    p.add_argument("--dry-run", action="store_true",
                   help="Pass --dryrun to aws s3 sync (no uploads); still builds manifest locally")
    p.add_argument("--manifest-only", action="store_true",
                   help="Skip syncing files; only rebuild and upload the manifest")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    profile = None if os.environ.get("AWS_ACCESS_KEY_ID") else args.profile

    if not args.manifest_only:
        if not LOCAL_RAW.exists():
            log.error(f"{LOCAL_RAW} does not exist — nothing to upload.")
            sys.exit(1)
        if not LOCAL_WEB.exists():
            log.error(f"{LOCAL_WEB} does not exist — nothing to upload.")
            sys.exit(1)

        log.info("Syncing raw JPEGs and WebP files in parallel…")
        p_raw = run_sync(LOCAL_RAW, f"s3://{S3_BUCKET}/{S3_PREFIX_RAW}", "image/jpeg", "*.jpg", profile, args.dry_run)
        p_web = run_sync(LOCAL_WEB, f"s3://{S3_BUCKET}/{S3_PREFIX_WEB}", "image/webp", "*.webp", profile, args.dry_run)

        # Read both pipes line-by-line in threads, updating progress bars in real time
        def _drain(proc: subprocess.Popen, bar: tqdm) -> int:
            for line in proc.stdout:
                if line.startswith("upload:"):
                    bar.update(1)
            proc.wait()
            return bar.n

        with tqdm(desc="  raw ", unit="file", position=0) as bar_raw, \
             tqdm(desc="  web ", unit="file", position=1) as bar_web:
            with ThreadPoolExecutor(max_workers=2) as pool:
                f_raw = pool.submit(_drain, p_raw, bar_raw)
                f_web = pool.submit(_drain, p_web, bar_web)
                n_raw = f_raw.result()
                n_web = f_web.result()

        for proc, n, label in [(p_raw, n_raw, "raw"), (p_web, n_web, "web")]:
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, f"aws s3 sync {label}")
        log.info(f"Sync complete — raw: {n_raw:,} files, web: {n_web:,} files")

    if args.dry_run:
        log.info("Dry run — skipping manifest build.")
        return

    df = build_manifest()
    if df.empty:
        log.warning("No .webp files found — manifest not uploaded.")
        return

    boto_session = (boto3.Session() if os.environ.get("AWS_ACCESS_KEY_ID")
                    else boto3.Session(profile_name=args.profile))
    s3 = boto_session.client("s3")
    upload_manifest(s3, df)

    log.info("Done.")


if __name__ == "__main__":
    main()
