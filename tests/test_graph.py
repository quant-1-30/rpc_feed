#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from rpc_feed.core.feed import bt_feed


if __name__ == "__main__":
    
   # asset
   # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/minutes/assets/asset.csv")
   # xml = "../xml/asset.graphml"
   # bt_feed.load(xml, dataset_path, prefix="csv", parallel=False)

   #  # calendar
   #  dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/calendar/calendar.csv")
   #  xml = "../xml/calendar.graphml"
   #  bt_feed.load(xml, dataset_path, prefix=".csv", parallel=False) 

   # struct
   #  dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/struct/202508") # tick
   #  xml = "../xml/tick.graphml"
   #  bt_feed.load(xml, dataset_path, prefix=".01", _filter="asset")
    
    dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/struct/202508") # fund
    xml = "../xml/fund.graphml"
    bt_feed.load(xml, dataset_path, prefix=".01", _filter="fund")

   # # csv
   # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/csv/stock/2022") # tick
   # xml = "../xml/csv.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", _filter="asset")

   # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/csv/fund/2005") # fund
   # xml = "../xml/csv_fund.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", _filter="fund")
