# cdef class Index:

#     def __init__(self):
#         self._buf_date = np.empty(CHUNK_SIZE, dtype=np.int32)
#         self._buf_open = np.empty(CHUNK_SIZE, dtype=np.int32)
#         self._buf_high = np.empty(CHUNK_SIZE, dtype=np.int32)
#         self._buf_low = np.empty(CHUNK_SIZE, dtype=np.int32)
#         self._buf_close = np.empty(CHUNK_SIZE, dtype=np.int32)
#         self._buf_volume = np.empty(CHUNK_SIZE, dtype=np.int64)
#         self._buf_amount = np.empty(CHUNK_SIZE, dtype=np.int64)

#     async def __call__(self, int start_date, int end_date, list sids=None):
#         cdef:
#             int i = 0
#             object row, stream
#             bytes r_sid, last_sid = b''

#         async with async_ops as ctx:
#             stmt = select(
#                 Benchmark.sid, Benchmark.date,
#                 cast(func.round(Benchmark.open * MULT), Integer),
#                 cast(func.round(Benchmark.high * MULT), Integer),
#                 cast(func.round(Benchmark.low * MULT), Integer),
#                 cast(func.round(Benchmark.close * MULT), Integer),
#                 cast(func.round(Benchmark.volume * MULT), BigInteger),
#                 cast(func.round(Benchmark.amount * MULT), BigInteger),
#             ).where(Benchmark.date.between(start_date, end_date)).order_by(Benchmark.sid, Benchmark.date)
            
#             if sids: stmt = stmt.where(Benchmark.sid.in_(sids))
            
#             stream_wrap = await ctx.on_query(stmt)

#             async with stream_wrap as stream_proxy:
#                 async for row in stream_proxy:
#                     r_sid = row[0]

#                     if (last_sid and r_sid != last_sid) or i >= CHUNK_SIZE:
#                         yield self._flush(i, last_sid)
#                         i = 0

#                     self._buf_date[i] = row[1]
#                     self._buf_open[i] = row[2]
#                     self._buf_high[i] = row[3]
#                     self._buf_low[i] = row[4]
#                     self._buf_close[i] = row[5]
#                     self._buf_volume[i] = row[6]
#                     self._buf_amount[i] = row[7]
#                     last_sid = r_sid
#                     i += 1

#                 if i > 0: yield self._flush(i, last_sid)

#     cdef object _flush(self, int count, bytes sid):
#         cdef dict metadata
#         cdef object batch
        
#         batch = pa.RecordBatch.from_arrays(
#             [
#                 pa.array(self._buf_date[:count], type=pa.int32()),
#                 pa.array(self._buf_open[:count], type=pa.int32()),
#                 pa.array(self._buf_high[:count], type=pa.int32()),
#                 pa.array(self._buf_low[:count], type=pa.int32()),
#                 pa.array(self._buf_close[:count], type=pa.int32()),
#                 pa.array(self._buf_volume[:count], type=pa.int64()),
#                 pa.array(self._buf_amount[:count], type=pa.int64()),
#             ],
#             names=["tick", "open", "high", "low", "close", "volume", "amount"]
#         )
#         metadata = {
#             b"sid": sid,
#             b"rpc_type": b"index"
#         }
#         batch = batch.replace_schema_metadata(metadata) # zero_copy
#         return batch_to_resp(batch)

cdef class Index:

    def __init__(self):
        root = Path(os.getenv("DUCKDATASET")).expanduser()
        self.dataset_root = os.path.join(root, "benchmark")

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None):
        """
            Hive 分区裁剪 (Partition Pruning) + 谓词下推 
        """
        cdef int32_t start_year = start_date // 10000
        cdef int32_t end_year = end_date // 10000
        cdef int64_t start_ts = intdt2ts(start_date)
        cdef int64_t end_ts = intdt2ts(end_date)

        # LazyFrame
        glob_path = f"{self.dataset_root}/*/*/*/*/*.parquet" # better than "{self.dataset_root}/**/*.parquet"

        lf = pl.scan_parquet(
            glob_path,
            hive_partitioning=True
        )

        lf = lf.filter(pl.col("year").cast(pl.Int32).is_between(start_year, end_year))
        
        if sids:
            # # Polars executionPlan cast number to string
            # sids_str = [s.decode("utf-8") for s in sids]
            # lf = lf.filter(pl.col("sid").cast(pl.Utf8).is_in(sids_str)) 
            sids_int = [int(s.decode("utf-8")) for s in sids]
            lf = lf.filter(pl.col("sid").cast(pl.Int32).is_in(sids_int))

        lf = lf.filter(pl.col("tick").cast(pl.Int32).is_between(start_ts, end_ts))
            
        lf = lf.select([
            pl.col("sid"),
            pl.col("tick"), # pl.col("tick").alias("tick").cast(pl.Int32)
            pl.col("open"), # (pl.col("open") * MULT).round().cast(pl.Int32)
            pl.col("high"),
            pl.col("low"),
            pl.col("close"),
            pl.col("volume"),
            pl.col("amount"),
        ]).sort(["sid", "tick"])

        # Polars collect C/Rust block api
        df = await asyncio.to_thread(lf.collect)

        if df.height == 0:
            return  

        # Zero-Copy to RecordBatch 
        for (sid_val,), group_df in df.group_by(["sid"]):
            arrow_table = group_df.drop("sid").to_arrow()
            sid_str_6 = str(sid_val).zfill(6)
 
            for batch in arrow_table.to_batches(max_chunksize=CHUNK_SIZE):
                metadata = {
                    b"sid": sid_str_6.encode('utf-8'),
                    b"rpc_type": b"index"
                }
                batch = batch.replace_schema_metadata(metadata)
                
                yield batch_to_resp(batch)
                