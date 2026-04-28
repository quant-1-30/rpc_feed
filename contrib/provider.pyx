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


cdef class TradingCalendar:
    """Calendar provider base class

    Provide calendar data.
    """
    def __init__(self): # __cinit__ used for memory allocate in C
        self._buf_date = np.empty(CHUNK_SIZE, dtype=np.int32)

    async def __call__(self, int start_date, int end_date, list sids=[]):
        """Get calendar of certain market in given time range.

        Parameters
        ----------
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.
        freq : str
            time frequency, available: year/quarter/month/week/day.
        future : bool
            whether including future trading day.

        Returns: List[int]
        """ 
        cdef int i    
        cdef object row, stream, result

        async with async_ops as ctx:
            stmt = select(Benchmark.date).distinct().where(
                    Benchmark.date.between(start_date, end_date)
            ).order_by(Benchmark.date)

            stream_wrap = await ctx.on_query(stmt) # stream wrap

            async with stream_wrap as stream_proxy:
                async for row in stream_proxy.scalars():
                    r_sid = row

                    if i >= CHUNK_SIZE:
                        yield self._flush(i)
                        i = 0

                    self._buf_date[i] = row
                    i += 1

                if i > 0:
                    yield self._flush(i)

    cdef object _flush(self, int count): # protobuf extend slice copy 
        cdef dict metadata
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(self._buf_date[:count], type=pa.int32())
            ],
            names=["date"]
        )
        metadata = {
            # b"tz_info": meta.tz_info,
            b"rpc_type": b"calendar"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


cdef class Experiment:

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=[]):
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query_experiment(req, EXPERIMENT_TEMPLATE):
                if batch.num_rows == 0: continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print("_process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print("_process_batch CancelledError")
                    raise 

    cdef _flush(self, bytes sid, object batch):
        metadata = {
        b"sid": sid,
        b"rpc_type": b"experiment"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)

cdef class Close:

    async def __call__(self, int start_date, int end_date, list sids=[]):
        """Get dataset data.

        Parameters
        ----------

        Returns
        ----------
        # SELECT * FROM stock
        # WHERE datetime >= TIMESTAMP '2024-06-01 09:30:00'
        # DuckDB '2024-06-01 09:30:00' TIMESTAMP align with Parquet datetime column
        # hive_partitioning  --- automate path to key=value in partition cols 
        """
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()
            int32_t num_rows  
            bytes sid

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, CLOSE_TEMPLATE):
                num_rows = batch.num_rows
                if num_rows == 0:
                    continue
                
                sid_array = batch.column("sid").to_numpy(zero_copy_only=False)

                c_indices = np.where(sid_array[:num_rows-1] != sid_array[1:])[0] + 1
                s_indices = np.insert(c_indices, 0, 0)
                e_indices = np.append(c_indices, len(sid_array))

                for start, end in zip(s_indices, e_indices):
                    sid = sid_array[start]
                    frame = self._flush(sid, batch.slice(start, end - start))
                    yield frame

    cdef object _flush(self, bytes sid, object batch):
        cdef dict metadata
        
        metadata = {
            b"sid": sid,
            b"rpc_type": b"close"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


            # from sqlalchemy.orm import load_only  # load_only 在 orm 模块中
            # stmt = select(Asset).options(
            #     load_only(Asset.sid, Asset.name, Asset.first_trading, Asset.delist) # only load necessary columns
            # )

 cdef class Index:

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None):
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, TICK_TEMPLATE):
                if batch.num_rows == 0: continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print("_process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print("_process_batch CancelledError")
                    raise 
    
    def _process_batch(self, object batch):
        cdef object sid_col = batch.column("sid")
        cdef Py_ssize_t num_rows = len(sid_col)
        
        cdef tuple indices
        cdef object s_indices, e_indices, slice_batch
        cdef Py_ssize_t start, end
        
        if num_rows == 0: 
            return
            
        indices = _slice_by_sid(sid_col, num_rows)
        s_indices = indices[0]
        e_indices = indices[1]

        for start_scalar, end_scalar in zip(s_indices, e_indices):
            start = start_scalar.as_py()
            end = end_scalar.as_py()
            sid = sid_col[start].as_py()
            slice_batch = batch.slice(start, end - start)
            yield self._flush(sid, slice_batch)

    cdef _flush(self, bytes sid, object batch):
        metadata = {
        b"sid": sid,
        b"rpc_type": b"index"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


cdef class DailyRet:

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=[]):
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, DAILY_RET_TEMPLATE):
                if batch.num_rows == 0: 
                    continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print("DailyRet _process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print("DailyRet _process_batch CancelledError")
                    raise 

    def _process_batch(self, object batch):
        """PyArrow Compute avoid to_numpy memory """

        cdef tuple indices
        cdef object s_indices, e_indices, slice_batch
        cdef Py_ssize_t start, end
        
        if num_rows == 0: 
            return
            
        indices = _slice_by_sid(sid_col, num_rows)
        s_indices = indices[0]
        e_indices = indices[1]

        for start_scalar, end_scalar in zip(s_indices, e_indices):
            start = start_scalar.as_py()
            end = end_scalar.as_py()
            sid = sid_col[start].as_py()
            
            slice_batch = batch.slice(start, end - start)
            yield self._flush(sid, slice_batch)

    cdef object _flush(self, bytes sid, object batch):
        cdef dict metadata = {
            b"sid": sid,
            b"rpc_type": b"daily_ret"  # 指定 rpc_type
        }
        batch = batch.replace_schema_metadata(metadata) # Zero-copy 替换 metadata
        return batch_to_resp(batch)
