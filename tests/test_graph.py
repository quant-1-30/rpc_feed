#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse

from rpc_feed.core.feed import bt_feed


def parse_args():
    parser = argparse.ArgumentParser(
        description='Backtest dataset Program',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='dataset', help='Commands')
    
    # dataset command
    dataset_parser = subparsers.add_parser('--xml', 
                                       #  required=True,
                                        help='graph xml')
    
    dataset_parser.add_argument('--dataset', '-d',
                           nargs='+',  # Accept multiple symbols
                           required=True,
                           help='calendar, asset, minute, adjustment, rightment')
    
    dataset_parser.add_argument('--dataset_path',
                           default='1',
                        #    choices=['1m', '5m', '15m', '1h', '4h', '1d'],
                           help='root path of dataset')

    dataset_parser.add_argument('--filter', '-f',
                           default=False,
                        #    choices=['1m', '5m', '15m', '1h', '4h', '1d'],
                           help='0 / 3 / 6 ')
    
    dataset_parser.add_argument('--parallel', '-n',
                           default='1',
                        #    choices=['1m', '5m', '15m', '1h', '4h', '1d'],
                           help='ncpu cores')


if __name__ == "__main__":

   #  args = parse_args()
   #  print(args)
    
   #  dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/calendar")
   #  xml = "../xml/calendar.graphml"
   #  bt_feed.load(xml, dataset_path, prefix=".csv")

   #  dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/assets")
   #  xml = "../xml/asset.graphml"
   #  bt_feed.load(xml, dataset_path, prefix="csv")
    
   #  dataset_path = os.path.join(os.path.expanduser("~"), "/Volumes/hengxin/quant/raw_data/minutes/201911/sh/minline/sh600225.01")
    dataset_path = os.path.join(os.path.expanduser("~"), "/Volumes/hengxin/quant/raw_data/minutes")
    xml = "../xml/tick.graphml"
    bt_feed.load(xml, dataset_path, prefix=".01", _filter="asset")
    