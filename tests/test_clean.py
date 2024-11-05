import pandas as pd
import os


if __name__ == "__main__":

    dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/assets/sz_delist.csv")
    # dataset_path = os.path.join(os.path.expanduser("~"), "Downloads/quant/assets/sz_assets.csv")

    df = pd.read_csv(dataset_path)
    # fill 0
    df["sid"] = df["sid"].apply(lambda x: f'{x:06}')

    if "delist" in df.columns:
        df["delist"] = df["delist"].apply(lambda x: int(x.replace("-", "")))
    if "first_trading" in df.columns:
        df["first_trading"] = df["first_trading"].apply(lambda x: int(x.replace("-", "")))

    df.to_csv("sz_delist_clean.csv", index=False)

