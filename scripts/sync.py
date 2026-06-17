import os
import shutil
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from dotenv import load_dotenv
from pathlib import Path

def compact_and_merge():
    source_path = Path(os.getenv("SpiderDataset")).expanduser()
    target_path = Path(os.getenv("SyncDataset")).expanduser()

    if not source_path.exists():
        print(f"Souce root {source_path} does not exist. Nothing to compact.")
        return

    # source_path/year=2026/quarter=Q2/sid=300059/date=202606
    sub_dirs = set(f.parent for f in source_path.rglob("*.parquet"))
    
    if not sub_dirs:
        print("No new parquet files found in staging.")
        return

    for sdir in sub_dirs:
        rel_path = sdir.relative_to(source_path) # key
        tdir = target_path / rel_path

        print(f"Processing partition: {rel_path}")

        # all reck parquet
        all_table = ds.dataset(sdir, format="parquet").to_table()
        df = all_table.to_pandas()

        # aggregate exist parquet
        target_file = tdir / "part-0.parquet"
        if target_file.exists():
            df_target = pq.read_table(target_file).to_pandas()
            df_combined = pd.concat([df_target, df], ignore_index=True)
        else:
            df_combined = df

        df_combined = df_combined.drop_duplicates(subset=['tick'], keep='last')
        df_combined = df_combined.sort_values('tick').reset_index(drop=True)

        fields = []
        for col in df_combined.columns:
            if col == "datetime":
                fields.append(pa.field(col, pa.timestamp("ms", tz="UTC")))
            else:
                fields.append(pa.field(col, pa.from_numpy_dtype(df_combined[col].dtype)))
        schema = pa.schema(fields)

        tdir.mkdir(parents=True, exist_ok=True)
        
        with tempfile.NamedTemporaryFile(
            dir=tdir, 
            prefix="compacting_", 
            suffix=".parquet", 
            delete=False
        ) as tmp:
            temp_file_path = Path(tmp.name)
            try:
                final_table = pa.Table.from_pandas(df_combined, schema=schema, preserve_index=False)
                pq.write_table(final_table, temp_file_path)
                
                tmp.close()
                # atomic replace 
                temp_file_path.replace(target_file)
            except Exception as e:
                if temp_file_path.exists():
                    temp_file_path.unlink()
                raise e
            finally:
                tmp.close()

        for f in sdir.glob("*.parquet"):
            f.unlink()
        print(f" -> Merged {len(df_combined)} rows into {target_file}")

    # cleanup_empty_dirs(source_path)
    print("Compaction finished successfully.")

def cleanup_empty_dirs(path: Path):
    if not path.is_dir():
        return
    for sub in path.iterdir():
        if sub.is_dir():
            cleanup_empty_dirs(sub)
    if not any(path.iterdir()):
        path.rmdir()


if __name__ == "__main__":

    load_dotenv()

    compact_and_merge()
