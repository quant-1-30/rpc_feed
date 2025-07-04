#! /usr/bin/env python3

import os
import pandas as pd
import asyncio
from rpc_feed.core.datasets.providers import _providers

def preprocess(path: str):
    delist_df = pd.read_csv(path, dtype={"sid": str})
    meta = delist_df.to_dict(orient="records") # row to dict
    return meta 


if __name__ == "__main__":
    # update sid
    path = os.path.join(os.path.expanduser("~"), "Downloads/minutes/assets/delist.csv")
    meta = preprocess(path)
    asyncio.run(_providers["update"](meta))

