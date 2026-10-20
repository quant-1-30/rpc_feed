#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import sys
import asyncio
import argparse
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from rpc_feed.core.gateway.pg.operator import async_ops


def load_asset_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(
        Path(csv_path).expanduser().resolve(),
        index_col=False,
        dtype={"sid": str, "merger": str, "ratio": float},
    )
    for col in ("sid", "name", "merger"):
        if col in df.columns:
            df[col] = df[col].where(df[col].isna(), df[col].astype(str).str.encode("utf-8"))
    df = df.drop_duplicates(subset=["sid"], keep="first")
    return df.astype(object).where(df.notna(), None)


def build_bulk_update(df: pd.DataFrame):
    records = [r for r in df.to_dict(orient="records") if r.get("sid") and r.get("delist") is not None]
    if not records:
        return None, {}

    value_expr = []
    params = {}
    for i, row in enumerate(records):
        sid_k, delist_k = f"sid_{i}", f"delist_{i}"
        value_expr.append(f"(:{sid_k}, CAST(:{delist_k} AS INTEGER))")
        params[sid_k] = row["sid"]
        params[delist_k] = int(row["delist"])

    stmt = text(
        f"""
        UPDATE asset
        SET delist = v.delist
        FROM (VALUES {', '.join(value_expr)}) AS v(sid, delist)
        WHERE asset.sid = v.sid
        """
    )
    return stmt, params


async def run_asset_job(csv_path: str, mode: str) -> None:
    df = load_asset_csv(csv_path)
    async with async_ops as ctx:
        if mode == "insert":
            await ctx.on_insert("asset", df)
            print(f"[asset_writer] Inserted {len(df)} asset rows.")
        else:
            stmt, params = build_bulk_update(df)
            if stmt is not None:
                await ctx.on_execute(stmt, params)
            print(f"[asset_writer] Updated delist status for {len(params) // 2} asset rows.")
    await async_ops.cleanup()


if __name__ == "__main__":

    """
        PostgreSQL asset 元数据离线维护工具(用于冷启动导入或退市日期批量更新;
        日常增量由 Plugin/spider 的 asset/adj/rgt 爬虫直接入库):
        用法示例:
            poetry run python scripts/asset_writer.py --csv ~/Downloads/assets.csv --mode insert
            poetry run python scripts/asset_writer.py --csv ~/Downloads/delist_assets.csv --mode update-delist
    """

    load_dotenv()
    parser = argparse.ArgumentParser(description="PostgreSQL asset table import & delist update tool")
    parser.add_argument("--csv", required=True, help="CSV 文件路径")
    parser.add_argument("--mode", choices=("insert", "update-delist"), default="insert")
    args = parser.parse_args()
    asyncio.run(run_asset_job(args.csv, args.mode))
