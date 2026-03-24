#!/usr/bin/env python3
"""
Rename S3 objects whose direction suffix is lowercase to uppercase.

Affected keys look like:
  photos/raw/{station}/{datetime}_e.jpg   →   photos/raw/{station}/{datetime}_E.jpg
  photos/web/{station}/{datetime}_e.webp  →   photos/web/{station}/{datetime}_E.webp

After renaming, the manifest (photos/manifest.parquet) is updated in place so
that s3_key_raw, s3_key_webp, and direction columns stay consistent.

Usage:
  python scripts/fix_lowercase_directions.py              # dry run (default)
  python scripts/fix_lowercase_directions.py --apply      # apply changes
  python scripts/fix_lowercase_directions.py --profile mco --apply
"""

import argparse
import logging
import os
import re
from io import BytesIO

import boto3
import botocore.exceptions
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

S3_BUCKET    = "mco-mesonet"
MANIFEST_KEY = "photos/manifest.parquet"
PREFIXES     = ["photos/raw/", "photos/web/"]

# Matches keys like photos/raw/csktbira/2026-03-20T090000_e.jpg
_KEY_RE = re.compile(
    r"^(photos/(?:raw|web)/[^/]+/\d{4}-\d{2}-\d{2}T\d{6}_)([A-Za-z]+)(\.\w+)$"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def iter_objects(s3, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        yield from page.get("Contents", [])


def rename_object(s3, old_key: str, new_key: str, apply: bool) -> None:
    if apply:
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={"Bucket": S3_BUCKET, "Key": old_key},
            Key=new_key,
            MetadataDirective="COPY",
        )
        s3.delete_object(Bucket=S3_BUCKET, Key=old_key)
    log.info(f"  {'RENAMED' if apply else 'WOULD RENAME'}  {old_key}  →  {new_key}")


def fix_manifest(s3, renames: dict[str, str], apply: bool) -> None:
    """Update s3_key_raw, s3_key_webp, and direction columns in the manifest."""
    buf = BytesIO()
    try:
        s3.download_fileobj(S3_BUCKET, MANIFEST_KEY, buf)
    except botocore.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            log.warning("No manifest found — skipping manifest update.")
            return
        raise

    buf.seek(0)
    df = pd.read_parquet(buf)
    original_len = len(df)
    original_raw = df["s3_key_raw"].copy()

    df["s3_key_raw"]  = df["s3_key_raw"].map(lambda k: renames.get(k, k))
    df["s3_key_webp"] = df["s3_key_webp"].map(lambda k: renames.get(k, k))
    # Re-derive direction from the (now corrected) webp key
    df["direction"] = df["s3_key_webp"].str.extract(r"_([A-Za-z]+)\.webp$")[0]

    changed = (df["s3_key_raw"] != original_raw).sum()
    log.info(f"Manifest: {changed:,} of {original_len:,} rows updated")

    if apply:
        out = BytesIO()
        schema = pa.schema([
            pa.field("station",     pa.string()),
            pa.field("direction",   pa.string()),
            pa.field("datetime",    pa.string()),
            pa.field("s3_key_raw",  pa.string()),
            pa.field("s3_key_webp", pa.string()),
        ])
        pq.write_table(
            pa.Table.from_pandas(df, schema=schema, preserve_index=False),
            out, compression="snappy",
        )
        out.seek(0)
        s3.upload_fileobj(out, S3_BUCKET, MANIFEST_KEY,
                          ExtraArgs={"ContentType": "application/octet-stream"})
        log.info(f"Manifest saved → s3://{S3_BUCKET}/{MANIFEST_KEY}")
    else:
        log.info("Dry run — manifest not written.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="mco",
                   help="AWS profile (default: %(default)s; ignored if AWS_ACCESS_KEY_ID is set)")
    p.add_argument("--apply", action="store_true",
                   help="Actually rename objects and update the manifest (default: dry run)")
    args = p.parse_args()

    session = (boto3.Session() if os.environ.get("AWS_ACCESS_KEY_ID")
               else boto3.Session(profile_name=args.profile))
    s3 = session.client("s3")

    renames: dict[str, str] = {}   # old_key → new_key

    # ── Find objects needing rename ───────────────────────────────────────────
    for prefix in PREFIXES:
        log.info(f"Scanning s3://{S3_BUCKET}/{prefix} …")
        for obj in iter_objects(s3, prefix):
            key = obj["Key"]
            m = _KEY_RE.match(key)
            if not m:
                continue
            base, direction, ext = m.group(1), m.group(2), m.group(3)
            if direction == direction.upper():
                continue   # already uppercase
            new_key = f"{base}{direction.upper()}{ext}"
            renames[key] = new_key

    if not renames:
        log.info("No lowercase-direction objects found — nothing to do.")
        return

    log.info(f"Found {len(renames):,} objects to rename{'' if args.apply else ' (dry run)'}:")

    # ── Rename objects ────────────────────────────────────────────────────────
    for old_key, new_key in sorted(renames.items()):
        rename_object(s3, old_key, new_key, apply=args.apply)

    # ── Update manifest ───────────────────────────────────────────────────────
    log.info("Updating manifest …")
    fix_manifest(s3, renames, apply=args.apply)

    log.info(f"Done. {len(renames):,} objects {'renamed' if args.apply else 'would be renamed'}.")


if __name__ == "__main__":
    main()
