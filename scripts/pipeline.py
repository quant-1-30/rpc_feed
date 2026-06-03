#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import multiprocessing

try:
    multiprocessing.set_start_method('spawn', force=True)
    print("✅ Multiprocessing start method set to 'spawn'")
except RuntimeError:
    pass # 如果已经设置过则忽略

import argparse

from pathlib import Path
from dotenv import load_dotenv

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

    load_dotenv()

    args = parse_args()

    # struct
    # dataset_path = Path("~/Downloads/rsync/202604").expanduser()
    # xml = "../xml/tick.graphml"
    # bt_feed.load(xml, str(dataset_path), prefix=".01_stock")

    # dataset_path = Path("~/Downloads/rsync/202604").expanduser() 
    # xml = "../xml/fund.graphml"
    # bt_feed.load(xml, str(dataset_path), prefix=".01_fund")
   
    # dataset csv
    year = 2007
    # dataset_path = Path(f"~/Downloads/raw_data/csv/stock/{year}").expanduser()
    # xml = "../xml/tick_csv.graphml"
    # bt_feed.load(xml, str(dataset_path), prefix="csv_stock", parallel=True)
    # print(f"Finished loading {year} stock csv data")
   
    dataset_path = Path(f"~/Downloads/raw_data/csv/fund/{year}").expanduser()
    xml = "../xml/fund_csv.graphml"
    bt_feed.load(xml, str(dataset_path), prefix="csv_fund")

    # # benchmark csv
    # dataset_path = Path("~/Downloads/raw/benchmark2026/regenerate").expanduser()
    # xml = "../xml/benchmark_csv.graphml"
    # bt_feed.load(xml, str(dataset_path), prefix="csv_benchmark")

    # # test
    # dataset_path = Path("~/Downloads/rsync/202604").expanduser()
    # xml = "../xml/test.graphml"
    # bt_feed.load(xml, str(dataset_path), prefix=".01_stock")
