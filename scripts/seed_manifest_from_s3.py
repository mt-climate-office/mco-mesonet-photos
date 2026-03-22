#!/usr/bin/env python3
"""
Build the manifest by scanning s3://mco-mesonet/photos/web/ and upload it.

Use this to seed or rebuild the manifest from scratch when local cache
files are unavailable.

Usage:
    python scripts/seed_manifest_from_s3.py [--profile mco] [--dry-run]
"""

import argparse
import logging
import re
from io import BytesIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Config ────────────────────────────────────────────────────────────────────

BUCKET       = "mco-mesonet"
WEBP_PREFIX  = "photos/web/"
MANIFEST_KEY = "photos/manifest.parquet"

MANIFEST_SCHEMA = pa.schema([
    pa.field("station",     pa.string()),
    pa.field("direction",   pa.string()),
    pa.field("datetime",    pa.string()),
    pa.field("s3_key_raw",  pa.string()),
    pa.field("s3_key_webp", pa.string()),
])

_WEBP_RE = re.compile(
    r"^photos/web/([^/]+)/(\d{4}-\d{2}-\d{2}T\d{6})_([A-Za-z]+)\.webp$"
)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="mco")
    p.add_argument("--dry-run", action="store_true",
                   help="Print row count without uploading")
    return p.parse_args()


def main():
    args = parse_args()
    s3 = boto3.Session(profile_name=args.profile).client("s3")

    log.info(f"Scanning s3://{BUCKET}/{WEBP_PREFIX} …")
    paginator = s3.get_paginator("list_objects_v2")
    rows = []
    total = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=WEBP_PREFIX):
        for obj in page.get("Contents", []):
            total += 1
            m = _WEBP_RE.match(obj["Key"])
            if not m:
                continue
            station, dt, direction = m.group(1), m.group(2), m.group(3)
            rows.append({
                "station":     station,
                "direction":   direction,
                "datetime":    dt,
                "s3_key_raw":  f"photos/raw/{station}/{dt}_{direction}.jpg",
                "s3_key_webp": obj["Key"],
            })
        if total % 50000 == 0 and total:
            log.info(f"  … {total:,} objects scanned, {len(rows):,} matched")

    log.info(f"Scanned {total:,} objects — {len(rows):,} manifest entries")

    if not rows:
        log.error("No matching objects found. Aborting.")
        return

    df = pd.DataFrame(rows, columns=MANIFEST_SCHEMA.names)

    if args.dry_run:
        log.info("[dry-run] skipping upload")
        print(df.head())
        return

    buf = BytesIO()
    pq.write_table(
        pa.Table.from_pandas(df, schema=MANIFEST_SCHEMA, preserve_index=False),
        buf, compression="snappy",
    )
    buf.seek(0)
    s3.upload_fileobj(buf, BUCKET, MANIFEST_KEY,
                      ExtraArgs={"ContentType": "application/octet-stream"})
    log.info(f"Manifest uploaded → s3://{BUCKET}/{MANIFEST_KEY} ({len(df):,} entries)")


if __name__ == "__main__":
    main()
