#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from rpc_feed.utils.io import upsert_daily_from_arrow_table

PRICE_SCALE = 100000

_HIVE_OPTS = (
    "hive_partitioning=true, "
    "hive_types={'sid': 'VARCHAR', 'year': 'VARCHAR', 'quarter': 'VARCHAR', 'date': 'VARCHAR'}"
)

MINUTE_LAKE_SCHEMA = pa.schema([
    pa.field("amount", pa.int64()),
    pa.field("close", pa.int64()),
    pa.field("datetime", pa.timestamp("ms", tz="UTC")),
    pa.field("high", pa.int64()),
    pa.field("low", pa.int64()),
    pa.field("open", pa.int64()),
    pa.field("tick", pa.int64()),
    pa.field("volume", pa.int64()),
])


def _strip(path: Path) -> str:
    return str(path).replace("'", "''")


def _scale_price(col: str, scale: int) -> str:
    return (
        f"(CASE WHEN typeof({col}) IN ('DOUBLE', 'FLOAT') "
        f"THEN round({col} * {scale}) ELSE {col} END)::BIGINT AS {col}"
    )


def _merge_sql(target_file: Path, staging_glob: Path, price_scale: int = PRICE_SCALE) -> str:
    prices = ", ".join(_scale_price(c, price_scale) for c in ("open", "high", "low", "close"))
    source = f"""
        SELECT
            round(amount)::BIGINT AS amount,
            round(volume)::BIGINT AS volume,
            {prices},
            tick::BIGINT AS tick,
            datetime::TIMESTAMPTZ AS datetime,
            year, quarter, sid, date,
            1 AS __pri
        FROM read_parquet('{_strip(staging_glob)}', {_HIVE_OPTS})
    """
    if target_file.exists():
        source += f"""
            UNION ALL BY NAME
            SELECT *, 0 AS __pri
            FROM read_parquet('{_strip(target_file)}', {_HIVE_OPTS})
        """
    return (
        "SELECT amount, close, datetime, high, low, open, tick, volume, year, quarter, sid, date "
        f"FROM ({source}) "
        "QUALIFY row_number() OVER (PARTITION BY tick ORDER BY __pri DESC) = 1 "
        "ORDER BY tick ASC"
    )


def cleanup_empty_dirs(path: Path) -> None:
    if not path.exists():
        return
    for p in sorted(path.rglob("*"), reverse=True):
        if p.is_dir() and not any(p.iterdir()):
            try:
                p.rmdir()
            except OSError:
                pass


def compact_and_merge(
    source_path: Path = None,
    target_path: Path = None,
    price_scale: int = PRICE_SCALE,
) -> int:
    src = source_path or os.getenv("SpiderDataset")
    tgt = target_path or os.getenv("SyncDataset")
    if not src or not tgt:
        print("Error: SpiderDataset or SyncDataset environment variable is not set.")
        return 0

    source_path = Path(src).expanduser().resolve()
    target_path = Path(tgt).expanduser().resolve()
    if not source_path.exists():
        print(f"Source root {source_path} does not exist. Nothing to compact.")
        return 0

    staging_by_dir = defaultdict(list)
    for f in source_path.rglob("*.parquet"):
        staging_by_dir[f.parent].append(f)
    if not staging_by_dir:
        print("No new parquet files found in staging.")
        return 0

    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone='UTC';")
    con.execute("SET preserve_insertion_order=true;")
    daily_batches = []

    try:
        for sdir in sorted(staging_by_dir):
            rel_path = sdir.relative_to(source_path)
            tdir = target_path / rel_path
            tdir.mkdir(parents=True, exist_ok=True)

            target_file = tdir / "part-0.parquet"
            merge_sql = _merge_sql(target_file, sdir / "*.parquet", price_scale)

            fd, tmp_name = tempfile.mkstemp(dir=tdir, prefix="compacting_", suffix=".parquet")
            os.close(fd)
            temp_file_path = Path(tmp_name)

            try:
                merged = con.execute(merge_sql).to_arrow_table()
                minute_tbl = merged.select(MINUTE_LAKE_SCHEMA.names).cast(MINUTE_LAKE_SCHEMA)
                pq.write_table(minute_tbl, temp_file_path, compression="snappy")
                temp_file_path.replace(target_file)
            except Exception:
                temp_file_path.unlink(missing_ok=True)
                raise

            daily_batches.append(merged)
            print(f" -> Merged {minute_tbl.num_rows} rows into {target_file}")

        if daily_batches:
            n_synced = upsert_daily_from_arrow_table(
                target_path, pa.concat_tables(daily_batches), con=con
            )
            print(f" -> Synced {n_synced} daily partition(s) for {target_path.name}")

        for files in staging_by_dir.values():
            for f in files:
                f.unlink(missing_ok=True)
        cleanup_empty_dirs(source_path)
    finally:
        con.close()

    print("Compaction finished successfully.")
    return len(staging_by_dir)


if __name__ == "__main__":
    load_dotenv()
    compact_and_merge()
