#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from rpc_feed.core.feed import bt_feed


if __name__ == "__main__":
    
   # #asset
   # dataset_path = Path("Downloads/minutes/assets/asset.csv").expanduser()
   # xml = "../xml/asset.graphml"
   # bt_feed.load(xml, dataset_path, prefix="csv", parallel=False)

   # # calendar
   # dataset_path = Path("Downloads/quant/data/calendar/calendar.csv").expanduser()
   # xml = "../xml/calendar.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", parallel=False) 

   #struct
   dataset_path = Path("Downloads/quant/data/struct/202508").expanduser() # tick
   xml = "../xml/tick.graphml"
   bt_feed.load(xml, dataset_path, prefix=".01", _filter="asset")
   
   dataset_path = Path("Downloads/quant/data/struct/202508").expanduser() # fund
   xml = "../xml/fund.graphml"
   bt_feed.load(xml, dataset_path, prefix=".01", _filter="fund")

   # # dataset csv
   # dataset_path = Path("Downloads/quant/data/csv/stock/2022").expanduser() # tick
   # xml = "../xml/csv.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", _filter="asset")

   # dataset_path = Path("Downloads/quant/data/csv/fund/2005").expanduser() # fund
   # xml = "../xml/csv_fund.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", _filter="fund")
