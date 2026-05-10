#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd


def rewrite_benchmark(path: str, file):
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

    return df.to_dict(orient="records")   


if __name__ == "__main__":

    path = "/Users/hengxinliu/Downloads/raw/benchmark2026"
    import os
    for file in os.listdir(path):
        if file.endswith(".csv"):
            # import pdb; pdb.set_trace()
            rewrite_benchmark(path, file)
