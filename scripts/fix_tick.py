def fix_quant_data():
    # 1. 初始化 DuckDB (内存模式)
    con = duckdb.connect(database=':memory:')
    
    # 2. 获取所有待处理的 parquet 文件
    print(f"正在扫描目录: {SOURCE_BASE} ...")
    all_files = list(SOURCE_BASE.glob("**/*.parquet"))
    total_files = len(all_files)
    print(f"共发现 {total_files} 个文件。")

    start_time = time.time()

    for idx, src_file in enumerate(all_files, 1):
        # 计算相对路径，以便在目标目录重建相同的层级
        rel_path = src_file.relative_to(SOURCE_BASE)
        dst_file = TARGET_BASE / rel_path
        
        # 3. 创建目标子目录
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            # 4. 执行 DuckDB SQL 转换
            # REPLACE 语法会自动替换同名列，保留其他所有原始列
            # 使用 ZSTD 压缩可以获得更好的空间比
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
            
            # 打印进度
            elapsed = time.time() - start_time
            print(f"[{idx}/{total_files}] 成功: {rel_path} | 累计耗时: {elapsed:.1f}s")

        except Exception as e:
            print(f"[{idx}/{total_files}] 失败: {rel_path} | 错误: {e}")

    print("-" * 50)
    print(f"任务完成！已保存至: {TARGET_BASE}")
    print(f"总计耗时: {time.time() - start_time:.2f} 秒")

if __name__ == "__main__":
    # 执行前检查
    if not SOURCE_BASE.exists():
        print(f"错误: 源目录 {SOURCE_BASE} 不存在！")
    else:
        fix_quant_data()
