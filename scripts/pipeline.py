#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
from pathlib import Path
from rpc_feed.core.rpc.feed import bt_feed


def parse_args():
    parser = argparse.ArgumentParser(
        description='Pipeline execution for data feed')

    parser.add_argument('--datapath', '-d',
                        default='../data/',
                        help='data to add to the system')

    parser.add_argument('--xml', '-x',
                        default='../xml/tick.graphml',
                        help='graph xml file')

    parser.add_argument('--prefix', '-pfx', 
                        default=.01,
                        help='file prefix for dataset')

    parser.add_argument('--filter', '-f', 
                        default='asset',
                        help='filter for dataset')

    parser.add_argument('--parallel', '-rv', action='store_true',
                        help='parallel execution')

    return parser.parse_args()


if __name__ == "__main__":

   args = parse_args()

   # # struct
   # dataset_path = Path("~/Downloads/raw_data/struct/202601").expanduser()
   # xml = "../xml/tick.graphml"
   # bt_feed.load(xml, str(dataset_path), prefix=".01_stock")
    
   # dataset_path = Path("~/Downloads/raw_data/struct/202601").expanduser() 
   # xml = "../xml/fund.graphml"
   # bt_feed.load(xml, str(dataset_path), prefix=".01_fund")
   
   # # dataset csv
   dataset_path = Path("~/Downloads/raw_data/csv/stock/2015").expanduser()
   xml = "../xml/tick_csv.graphml"
   bt_feed.load(xml, str(dataset_path), prefix="csv_stock")
   
   # dataset_path = Path("~/Downloads/raw_data/csv/fund/2022").expanduser()
   # xml = "../xml/fund_csv.graphml"
   # bt_feed.load(xml, str(dataset_path), prefix="csv_fund")
