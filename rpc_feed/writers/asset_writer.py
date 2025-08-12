#! /usr/bin/env python3

import os
import asyncio
import pandas as pd
from sqlalchemy import select, and_, or_ # SQLAlchemy 2.0.39 正确的导入方式
from rpc_feed.core.helper.schema import Asset
from rpc_feed.core.helper.operator import async_ops

def preprocess(path: str):
    delist_df = pd.read_csv(path, dtype={"sid": str})
    meta = delist_df.to_dict(orient="records") # row to dict
    return meta 


async def task(meta: dict):
    """Update asset data."""
    async with async_ops as ctx:
        for item in meta:
            stmt = select(Asset).where(Asset.sid == item["sid"])
            asset = await ctx.on_query_obj(stmt)
            if asset:
                asset[0].delist = item["delist"] # 如果记录存在，更新 delist 列
            else:
                asset = Asset(**item) # 如果记录不存在，插入新记录
            await ctx.on_insert_obj(asset)


if __name__ == "__main__":
    # update sid
    path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/asset/delist.csv")
    meta = preprocess(path)
    asyncio.run(task(meta))

