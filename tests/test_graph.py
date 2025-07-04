#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse

from rpc_feed.core.feed import bt_feed


# def parse_args():
#     parser = argparse.ArgumentParser(
#         description='Backtest dataset Program',
#         formatter_class=argparse.ArgumentDefaultsHelpFormatter
#     )
    
#     # Create subparsers for different commands
#     subparsers = parser.add_subparsers(dest='dataset', help='Commands')
    
#     # dataset command
#     dataset_parser = subparsers.add_parser('--xml', 
#                                        #  required=True,
#                                         help='graph xml')
    
#     dataset_parser.add_argument('--dataset', '-d',
#                            nargs='+',  # Accept multiple symbols
#                            required=True,
#                            help='calendar, asset, minute, adjustment, rightment')
    
#     dataset_parser.add_argument('--dataset_path',
#                            default='1',
#                         #    choices=['1m', '5m', '15m', '1h', '4h', '1d'],
#                            help='root path of dataset')

#     dataset_parser.add_argument('--filter', '-f',
#                            default=False,
#                         #    choices=['1m', '5m', '15m', '1h', '4h', '1d'],
#                            help='0 / 3 / 6 ')
    
#     dataset_parser.add_argument('--parallel', '-n',
#                            default='1',
#                         #    choices=['1m', '5m', '15m', '1h', '4h', '1d'],
#                            help='ncpu cores')


if __name__ == "__main__":
    
   #  args = parse_args()
   #  print(args)

   # asset
   # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/minutes/assets/asset.csv")
   # xml = "../xml/asset.graphml"
   # bt_feed.load(xml, dataset_path, prefix="csv", parallel=False)

    # calendar
    dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/data/calendar/calendar.csv")
    xml = "../xml/calendar.graphml"
    bt_feed.load(xml, dataset_path, prefix=".csv", parallel=False) 

   # struct
   #  dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/2025") # tick
   #  xml = "../xml/tick.graphml"
   #  bt_feed.load(xml, dataset_path, prefix=".01", _filter="asset")
    
   #  dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/2025") # fund
   #  xml = "../xml/fund.graphml"
   #  bt_feed.load(xml, dataset_path, prefix=".01", _filter="fund")

   # csv
   # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/minutes/csv/stock/2005") # tick
   # xml = "../xml/csv.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", _filter="asset")

   # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/minutes/csv/fund/2005") # fund
   # xml = "../xml/csv.graphml"
   # bt_feed.load(xml, dataset_path, prefix=".csv", _filter="fund")
