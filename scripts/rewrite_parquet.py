import time
import duckdb
from pathlib import Path


def fix_quant_data(SOURCE_BASE, TARGET_BASE, TICK_OFFSET):
    # 1. DuckDB inmemory connection
    con = duckdb.connect(database=':memory:')
    
    print(f"Scan {SOURCE_BASE} ...")
    all_files = list(SOURCE_BASE.glob("**/*.parquet"))
    total_files = len(all_files)
    print(f"found {total_files} files")

    start_time = time.time()

    for idx, src_file in enumerate(all_files, 1):
        # key relative path
        rel_path = src_file.relative_to(SOURCE_BASE)
        dst_file = TARGET_BASE / rel_path
        
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            # DuckDB SQL REPLACE and ZSTD 
            # DuckDB  Parquet default TIMESTAMP WITH TIME ZONE` (TIMESTAMPTZ) `INTERVAL 8 HOUR`
            query = f"""
                COPY (
                    SELECT * REPLACE (
                        CAST(tick - {TICK_OFFSET} AS BIGINT) AS tick,
                        datetime - INTERVAL 8 HOUR AS datetime
                    )
                    FROM read_parquet('{src_file}')
                ) TO '{dst_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
            """
            con.execute(query)
            
            elapsed = time.time() - start_time
            print(f"[{idx}/{total_files}] Success: {rel_path} | Elpased: {elapsed:.1f}s")

        except Exception as e:
            print(f"[{idx}/{total_files}] Failure: {rel_path} | Error: {e}")

    print("-" * 50)
    print(f"Completed: {TARGET_BASE}")
    print(f"Elapsed: {time.time() - start_time:.2f} second")


if __name__ == "__main__":
    SOURCE_BASE = Path("/Users/hengxinliu/Downloads/quant")
    TARGET_BASE = Path("/Users/hengxinliu/Downloads/fixed_quant")
    TICK_OFFSET = 8 * 3600  # utc offset 8 hours in seconds
    fix_quant_data(SOURCE_BASE, TARGET_BASE, TICK_OFFSET)
