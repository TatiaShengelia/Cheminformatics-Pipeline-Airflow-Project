"""S3 helpers for the chem pipeline.

- `get_pair_for_id`: locate the scaffold/r-groups pair for one known dataset id
  (used for manual/backfill runs, e.g. via the `dataset_id` param).
- `discover_new_pairs`: scan the whole input prefix, pair up scaffold+r_groups
  files by id, and filter down to what's new since the last successful run
  (or everything, if `overwrite=True`). Added in Step 2 for the weekly scan.
"""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


@dataclass
class DatasetPair:
    dataset_id: str
    scaffold_key: str
    r_groups_key: str
    last_modified: Optional[str] = None


def get_pair_for_id(hook: S3Hook, bucket: str, prefix: str, dataset_id: str,
                     scaffold_suffix: str, r_groups_suffix: str) -> DatasetPair:
    """Resolve the two expected object keys for a given dataset id and make
    sure both actually exist in the bucket."""
    scaffold_key = f"{prefix.rstrip('/')}/{dataset_id}{scaffold_suffix}"
    r_groups_key = f"{prefix.rstrip('/')}/{dataset_id}{r_groups_suffix}"

    if not hook.check_for_key(scaffold_key, bucket_name=bucket):
        raise FileNotFoundError(f"Missing scaffold file: s3://{bucket}/{scaffold_key}")
    if not hook.check_for_key(r_groups_key, bucket_name=bucket):
        raise FileNotFoundError(f"Missing r-groups file: s3://{bucket}/{r_groups_key}")

    return DatasetPair(dataset_id=dataset_id, scaffold_key=scaffold_key, r_groups_key=r_groups_key)


def read_csv_from_s3(hook: S3Hook, bucket: str, key: str) -> pd.DataFrame:
    obj = hook.get_key(key, bucket_name=bucket)
    raw = obj.get()["Body"].read()
    return pd.read_csv(io.BytesIO(raw))


def write_df_to_s3(hook: S3Hook, df: pd.DataFrame, bucket: str, key: str, overwrite: bool = True) -> None:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    hook.load_bytes(csv_bytes, key=key, bucket_name=bucket, replace=overwrite)


def output_exists(hook: S3Hook, bucket: str, output_prefix: str, dataset_id: str) -> bool:
    """Cheap existence check used to decide whether a dataset was already
    processed, so non-overwrite runs don't redo finished work."""
    marker_key = f"{output_prefix.rstrip('/')}/{dataset_id}_clustered.csv"
    return hook.check_for_key(marker_key, bucket_name=bucket)


def discover_all_pairs(hook: S3Hook, bucket: str, prefix: str,
                        scaffold_suffix: str, r_groups_suffix: str) -> dict[str, DatasetPair]:
    """List every object under `prefix` and pair up matching scaffold/r_groups
    files by their shared `<id>` stem. Files without a matching counterpart
    are logged and skipped (nothing to assemble yet)."""
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []

    scaffolds: dict[str, str] = {}
    r_groups: dict[str, str] = {}
    for key in keys:
        filename = key.rsplit("/", 1)[-1]
        if filename.endswith(scaffold_suffix):
            scaffolds[filename[: -len(scaffold_suffix)]] = key
        elif filename.endswith(r_groups_suffix):
            r_groups[filename[: -len(r_groups_suffix)]] = key

    pairs: dict[str, DatasetPair] = {}
    all_ids = set(scaffolds) | set(r_groups)
    for dataset_id in sorted(all_ids):
        if dataset_id not in scaffolds:
            logger.warning("Dataset %s has r_groups file but no scaffolds file - skipping", dataset_id)
            continue
        if dataset_id not in r_groups:
            logger.warning("Dataset %s has scaffolds file but no r_groups file - skipping", dataset_id)
            continue

        scaffold_meta = hook.head_object(scaffolds[dataset_id], bucket_name=bucket) or {}
        r_groups_meta = hook.head_object(r_groups[dataset_id], bucket_name=bucket) or {}
        last_modified = max(
            scaffold_meta.get("LastModified", datetime.min.replace(tzinfo=timezone.utc)),
            r_groups_meta.get("LastModified", datetime.min.replace(tzinfo=timezone.utc)),
        )
        pairs[dataset_id] = DatasetPair(
            dataset_id=dataset_id,
            scaffold_key=scaffolds[dataset_id],
            r_groups_key=r_groups[dataset_id],
            last_modified=last_modified.isoformat(),
        )
    return pairs


def filter_new_or_overwrite(
    hook: S3Hook,
    pairs: dict[str, DatasetPair],
    bucket: str,
    output_prefix: str,
    last_run_ts: Optional[str],
    overwrite: bool,
) -> list[DatasetPair]:
    """Decide which dataset ids to actually process this run.

    - overwrite=True  -> process every pair found, regardless of watermark.
    - overwrite=False -> process a pair if it's newer than `last_run_ts`
                         OR if it has never produced an output yet.
    """
    if overwrite:
        return list(pairs.values())

    watermark = pd.Timestamp(last_run_ts) if last_run_ts else pd.Timestamp.min.tz_localize("UTC")
    to_process = []
    for pair in pairs.values():
        is_new_upload = pd.Timestamp(pair.last_modified) > watermark
        already_processed = output_exists(hook, bucket, output_prefix, pair.dataset_id)
        if is_new_upload or not already_processed:
            to_process.append(pair)
    return to_process
