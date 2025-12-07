#! /usr/bin/env python3

import asyncio
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import select, and_, or_ , text # SQLAlchemy 2.0.39 正确的导入方式
from pathlib import Path

from rpc_feed.core.operator.pg.schema import Asset
from rpc_feed.core.operator.pg.operator import async_ops


def bulk_update(p_str: str):
    updates = pd.read_excel(p_str, engine="openpyxl", dtype={"sid": str})
    meta = updates.to_dict(orient="records") # row to dict
    if not meta:
        return 0

    # 构造参数占位符和参数字典
    value_expr = []
    params = {}
    
    for i, data in enumerate(meta):
        sid_param = f"sid_{i}"
        delist_param = f"delist_{i}"
        value_expr.append(f"(:{sid_param}, CAST(:{delist_param} AS INTEGER))")  # hint cast type
        params[sid_param] = data["sid"]
        params[delist_param] = int(data["delist"])  
    
    values_clause = ", ".join(value_expr)
    stmt = text(f"""
        UPDATE asset
        SET delist = v.delist
        FROM (VALUES {values_clause}) AS v(sid, delist)
        WHERE asset.sid = v.sid
    """) # postgres sql length has limit and need to split 
    return stmt, params


async def task(p_str: str):
    """Update asset data."""
    stmt, params = bulk_update(p_str)
    async with async_ops as ctx: # bulk_update_mappings 同步
        await ctx.on_execute(stmt, params)


if __name__ == "__main__":
    load_dotenv()

    # update sid
    path = Path("~/Downloads/raw_data/assets/delist.xlsx").expanduser()
    asyncio.run(task(path))

