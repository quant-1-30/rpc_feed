#! /usr/bin/env python3

import os
import asyncio
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import select, and_, or_ , text # SQLAlchemy 2.0.39 
from pathlib import Path

from bt_protocol.orm.market import Asset
from rpc_feed.core.gateway.pg.operator import async_ops


def write_benchmark(path: str, file):
    # engine="openpyxl"
    # encoding="utf-8" / "gbk" 
    df = pd.read_csv(os.path.join(path, file), encoding="utf-8")
    
    if df.empty:
        return 0

    mapping = {
        "SH.000001": "SH.1A0001",
        "SH.000688": "SH.1B0688",
        "SZ.399001": "SZ.2A01",
        "SZ.399006": "SZ.399006",
    }

    df["代码"] = df["代码"].map(mapping).fillna(df["代码"])

    file = file.replace(".csv", "")
    _p_str = os.path.join(path, "regenerate", f"{mapping[file]}.csv")
    df.to_csv(_p_str, index=False)


# def rewrite_asset():
    # # 0
    # # sz = pd.read_excel("/Users/hengxinliu/Downloads/raw_data/assets/sz.xls", engine="openpyxl")
    # sz = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/sz.csv", index_col=False, dtype={"sid": str})
    # sz["first_trading"] = pd.to_datetime(sz["first_trading"], errors="coerce")
    # sz["first_trading"] = sz["first_trading"].dt.strftime("%Y%m%d").astype(int)    

    # # # 6
    # sh = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/sh.csv", index_col=False, dtype={"sid": str})

    # # # 688
    # sh_kcb = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/sh_kcb.csv", index_col=False, dtype={"sid": str})

    # # # delist
    # delist = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/delist.csv", index_col=False, dtype={"sid": str})

    # # concat
    # assets = pd.concat([sz, sh, sh_kcb, delist])
    # assets["sid"] = assets["sid"].astype(str).str.zfill(6)
    # assets["delist"] = assets["delist"].fillna(0)
    # # df = assets.merge(delist, on=["sid"], how="left")

    # assets.to_csv("asset.csv", index=False)

    # suspend = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/终止上市.csv", index_col=False, dtype={"sid": str})
    # suspend["first_trading"] = pd.to_datetime(suspend["first_trading"], errors="coerce")
    # suspend["first_trading"] = suspend["first_trading"].dt.strftime("%Y%m%d").astype(int)    
    
    # suspend["delist"] = pd.to_datetime(suspend["delist"], errors="coerce")
    # suspend["delist"] = suspend["delist"].dt.strftime("%Y%m%d").astype(int)    
    
    # suspend["sid"] = suspend["sid"].astype(str).str.zfill(6)
    
    # suspend.to_csv("suspend.csv", index=False)
    
    # suspend_1 = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/delist.csv", index_col=False, dtype={"sid": str})
    # suspend_2 = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/suspend_assets.csv", index_col=False, dtype={"sid": str})

    # suspend = pd.concat([suspend_1, suspend_2])
    # suspend["sid"] = suspend["sid"].astype(str).str.zfill(6)
    # assets["delist"] = assets["delist"].fillna(0)
    # # df = assets.merge(delist, on=["sid"], how="left") 

    # suspend = pd.merge(
    #     suspend_1,
    #     suspend_2[["sid", "first_trading"]],  # 右表切片，只取关联键和目标列
    #     on="sid",
    #     how="left"                              # 左连接：保持 df_main 的行数和顺序完全不变
    # )
    # print(suspend)
    # suspend.to_csv("suspend.csv", index=False)


def writer_asset():
    assets = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/202605/asset.csv", index_col=False, dtype={"sid": str})
    suspend = pd.read_csv("/Users/hengxinliu/Downloads/raw_data/assets/202605/suspend.csv", index_col=False, dtype={"sid": str})

    suspend_sid = set(suspend["sid"])

    assets = assets[~assets["sid"].isin(suspend_sid)]

    df = pd.concat(
        [suspend, assets],
        ignore_index=True,
        copy=False,
    )
    # string --- bytes
    df.to_csv("market_assets.csv", index=False)
    df["sid"] = df["sid"].str.encode("utf-8")
    df["name"] = df["name"].str.encode("utf-8")
    dup_values = df.loc[df["sid"].duplicated(), "sid"]
    print("dup_values: ", len(dup_values))
    import pdb; pdb.set_trace()
    return df


def bulk_update(df: pd.DataFrame):
    meta = df.to_dict(orient="records") # row to dict
    if not meta:
        return 0

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

async def update_execute(df: pd.DataFrame):
    """Update asset data."""
    stmt, params = bulk_update(df)
    async with async_ops as ctx: 
        await ctx.on_execute(stmt, params)

async def _execute(table: str, df: pd.DataFrame):
    async with async_ops as ctx:
        await ctx.on_insert(table, df)


if __name__ == "__main__":
    load_dotenv()

    # insert sid
    df = writer_asset()
    asyncio.run(_execute("asset", df))

    # # write benchmark csv
    # path = "/Users/hengxinliu/Downloads/raw/benchmark2026"
    # for file in os.listdir(path):
    #     if file.endswith(".csv"):
    #         rewrite_benchmark(path, file)
