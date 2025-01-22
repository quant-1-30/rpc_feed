#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
from core.ops.schema import *
from core.ops.operator import async_ops

async def save_data(obj):
    resp = await async_ops.on_insert_obj(obj)


if __name__ == "__main__":

    adj_metadata = {"sid": "603676", "register_date": 20250101, "ex_date": 20250102, 
                "share": 10, "transfer": 10, "interest": 10}
    
    rgt_metadata = {"sid": "603676", "register_date": 20250101, "ex_date": 20250102, 
                "price": 10, "ratio": 10}

    # adjustment_obj = Adjustment(**adj_metadata)
    # asyncio.run(save_data(adjustment_obj))

    rightment_obj = Rightment(**rgt_metadata)
    asyncio.run(save_data(rightment_obj))
