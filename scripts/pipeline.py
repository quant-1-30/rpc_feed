#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import multiprocessing
from pathlib import Path
from dotenv import load_dotenv

try:
    multiprocessing.set_start_method("spawn", force=True)
except RuntimeError:
    pass

_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from rpc_feed.core.feed import bt_feed


def parse_args():
    parser = argparse.ArgumentParser(description="DAG ETL Pipeline execution for rpc_feed")
    parser.add_argument("--datapath", "-d", required=True, help="输入数据集目录或文件路径")
    parser.add_argument("--xml", "-x", default="xml/tick.graphml", help="GraphML 配置文件路径")
    parser.add_argument(
        "--prefix",
        "-p",
        default=".01_stock",
        help="<suffix>_<category>, e.g.: .01_stock / .01_fund / csv_stock / csv_fund / csv_benchmark",
    )
    parser.add_argument("--parallel", "-rv", action="store_true", default=True, help="loky mulitprocess")
    parser.add_argument("--no-parallel", dest="parallel", action="store_false", help="forbiden loky mulitprocess")
    return parser.parse_args()


if __name__ == "__main__":

    """
    DAG ETL CLI (.01  / .csv -> Parquet / PostgreSQL):
        poetry run python scripts/pipeline.py -x xml/tick.graphml -d ~/Downloads/rsync/202609 -p .01_stock --parallel
        poetry run python scripts/pipeline.py -x xml/fund.graphml -d ~/Downloads/rsync/202609 -p .01_fund --parallel
        poetry run python scripts/pipeline.py -x xml/benchmark_csv.graphml -d ~/Downloads/raw/benchmark -p csv_benchmark
    """
    load_dotenv()
    args = parse_args()
    dataset_path = str(Path(args.datapath).expanduser().resolve())
    xml_path = str(Path(args.xml).expanduser().resolve())
    bt_feed.load(xml_path, dataset_path, prefix=args.prefix, parallel=args.parallel)
