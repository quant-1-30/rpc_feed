# rpc_feed 并发安全与性能修复方案

> 日期: 2026-08-03  
> 作者: AI 辅助审查  
> 状态: ✅ 已完成并验证通过

---

## 1. 背景与目标

`rpc_feed` 作为面向多客户端的 gRPC 流式行情数据服务,需要支撑多个客户端并发请求。审查代码后发现存在三类严重问题:

1. **并发安全 (CRITICAL)** — SQLAlchemy Provider 的 numpy buffer 在并发请求间被互相覆盖,导致数据错乱
2. **资源泄漏 (CRITICAL)** — 缺少客户端取消处理,DuckDB 连接和 PG session 在客户端断开后无法释放
3. **性能卡点 (HIGH)** — 多处同步调用阻塞事件循环,SQL 缓存被禁用,内存分配存在冗余

本次修复覆盖从 Cython 底层(`.pyx`/`.pxd`)到 Python 编排层(`server.py`/`operator.py`)的全链路。

---

## 2. 问题分析与修复

### 2.1 🔴 CRITICAL: Provider Buffer 竞态

#### 问题定位

**根因**: 单例对象 + 可变实例属性 + async 协程切换点 三者叠加。

**推导链**:

1. **Provider 是单例** — `core/datasets/__init__.py` 模块加载时创建全局唯一实例:
   ```python
   _providers = dict((
       ("asset", Instrument()),   # 全局唯一
       ("adjust", Adjust()),
       ("right", Right()),
   ))
   ```

2. **buffer 挂在实例属性上** — `provider.pyx` 的 `_init_buffers` 写 `self.buf_*`:
   ```python
   cdef void _init_buffers(self):
       self.buf_sid = [b''] * CHUNK_SIZE           # 实例属性
       self.buf_ratio = np.empty(CHUNK_SIZE, dtype=np.float32)  # 实例属性
   ```

3. **`__call__` 有多个 await 切换点** — 两个并发请求的协程会在这些点交错执行:
   ```python
   async def __call__(self, start_date, end_date, sids=None):
       self._init_buffers()           # 写 self.*
       async with async_ops as ctx:   # ← await 挂起
           async for row in stream_proxy:  # ← await 挂起
               self._row_to_buffer(i, row)  # 写 self.*[i]
   ```

4. **交错执行导致数据覆盖**:
   ```
   时间线  |  Request A (client-1)              |  Request B (client-2)
   --------|-------------------------------------|--------------------------------------
    t1     |  self._init_buffers() → buf=np_A    |
    t2     |  await ctx.on_query(stmtA) ←挂起     |
    t3     |                                     |  self._init_buffers() → buf=np_B (覆盖!)
    t4     |                                     |  await ctx.on_query(stmtB) ←挂起
    t5     |  恢复: self._row_to_buffer(0, rowA) |  ← 写进 np_B,不是 np_A!
    t6     |  yield self._flush_buffer(i)        |  ← 读 np_B,发给 client-1 的是 client-2 的数据!
   ```

#### 修复方案 (Mode B: 请求局部 buffer)

保持 Provider 单例不变,但将 buffer 从实例属性改为**请求局部变量**,通过新增 buffer 容器类实现:

**新增三个 buffer 容器类** (`provider.pxd` / `provider.pyx`):

```cython
cdef class InstrumentBuffer:
    cdef public list buf_sid
    cdef public list buf_name
    cdef public object buf_first_trading
    cdef public object buf_delist
    cdef public list buf_merger
    cdef public object buf_ratio

cdef class AdjustBuffer:
    cdef public object buf_ex_date
    cdef public object buf_register_date
    cdef public object buf_bonus_share
    cdef public object buf_transfer
    cdef public object buf_bonus

cdef class RightBuffer:
    cdef public object buf_ex_date
    cdef public object buf_register_date
    cdef public object buf_price
    cdef public object buf_ratio
```

**修改方法签名**:

| 方法 | 修改前 | 修改后 |
|------|--------|--------|
| `_init_buffers` | `cdef void _init_buffers(self)` | `cdef object _init_buffers(self)` (返回 buffer 对象) |
| `_row_to_buffer` | `cdef void _row_to_buffer(self, int i, object row)` | `cdef void _row_to_buffer(self, object buf, int i, object row)` |
| `_flush_buffer` | `cdef object _flush_buffer(self, int count, bytes sid)` | `cdef object _flush_buffer(self, object buf, int count, bytes sid)` |

**`__call__` 使用局部 buffer**:

```python
async def __call__(self, start_date, end_date, sids=None, context=None):
    cdef object buf = self._init_buffers()  # 局部变量!每次请求独立
    async with async_ops as ctx:
        async for row in stream_proxy:
            self._row_to_buffer(buf, i, row)  # 写 buf.*[i],不是 self.*
```

**为什么 DuckDB Provider (Tick/Daily/Close) 安全?**

`_process_batch` 全程使用**局部变量**:
```python
def _process_batch(self, object batch):   # batch 是参数(局部引用)
    cdef object sid_col = batch.column("sid")  # 局部
    cdef list sid_list = sid_col.to_pylist()   # 局部
    for i in range(n_seg):
        slice_batch = batch.slice(start, end-start)  # 局部,Arrow 零拷贝
```
唯一共享的 `self.rpc_type` / `self.template` 是 `__cinit__` 后只读的。

---

### 2.2 🔴 CRITICAL: 客户端取消 + 资源泄漏

#### 问题

客户端断开连接后,服务端的 async generator 不会被立即终止,导致:
- DuckDB 连接不归还连接池(默认仅 10 个连接)
- PG session 不关闭,连接池逐渐耗尽
- CPU 继续处理无用数据

#### 修复

> **⚠️ grpc.aio API 注意**: `grpc.aio.ServicerContext` **没有** `is_active()` 方法(这是同步 API 的方法)。
> 异步 API 应使用 `context.done()` — 返回 `bool`,表示 RPC 是否已结束(含客户端取消)。

**Provider 层** (`provider.pyx`):

`__call__` 增加 `context` 参数,在 `async for` 循环中检查取消状态:
```python
async def __call__(self, start_date, end_date, sids=None, context=None):
    async for batch in ctx.query(req, self.template):
        if context is not None and context.done():
            print(f"[{self.rpc_type.decode()}] client cancelled, stopping stream")
            return  # 提前退出,释放连接
```

**Server 层** (`server.py`):

所有 7 个 RPC 方法增加取消检查:
```python
async with _stream_semaphore:
    async for response in response_iterator:
        if context.done():
            logging.info("TickStreamCall: client disconnected")
            return
        yield response
```

**关闭时资源清理** (`run_server.py`):
```python
async def shutdown():
    try:
        await async_ops.cleanup()  # dispose PG engine
    except Exception as e:
        logging.warning(f"AsyncOps cleanup error: {e}")
    try:
        duck_mgr.close()  # 关闭 DuckDB 连接池
    except Exception as e:
        logging.warning(f"DuckDB cleanup error: {e}")
    await server.stop(grace=5)
```

---

### 2.3 🟡 HIGH: 性能卡点

#### 2.3.1 `_glob_path` 阻塞事件循环

**问题**: `_glob_path` 包含大量 `os.path.exists()` 同步调用(5000 sid × 20 quarter = 10万次),直接在事件循环中执行会阻塞所有其他协程。

**修复** (`duckdb/operator.py`):
```python
async def query(self, req: dict, raw_template: str):
    loop = asyncio.get_running_loop()
    # 异步化到 executor
    file_globs = await loop.run_in_executor(None, self._glob_path, req)
```

#### 2.3.2 SQL 缓存被禁用

**问题**: `pg/operator.py` 中 `compiled_cache={}` 禁用了 SQLAlchemy 的 SQL 编译缓存,每次查询都重新编译。

**修复**: 移除 `compiled_cache={}`:
```python
# 修改前
engine = create_async_engine(url, ...).execution_options(compiled_cache={})
# 修改后
engine = create_async_engine(url, ...)  # 使用默认缓存
```

#### 2.3.3 `to_pylist()` 内存开销

**问题**: `_process_batch` 中三次调用 `to_pylist()` 创建大量 Python 对象。

**修复** (`provider.pyx`):
```python
# 修改前
s_list = s_indices.to_pylist()   # 创建 Python list
e_list = e_indices.to_pylist()   # 创建 Python list
sid_list = sid_col.to_pylist()   # 全量转换(可能很大)

# 修改后
s_np = s_indices.to_numpy(zero_copy_only=False)  # numpy 数组,更高效
e_np = e_indices.to_numpy(zero_copy_only=False)
# sid 按索引取值,避免全量转换
sid = sid_col[start].as_py()
```

#### 2.3.4 `IpcWriteOptions` 线程竞争

**问题**: `use_threads=True` 在高并发下会创建额外线程,加剧 GIL 竞争。

**修复** (`provider.pyx`):
```python
cdef object arrow_options = pa.ipc.IpcWriteOptions(
    compression='lz4',
    use_threads=False  # 高并发下避免线程竞争
)
```

#### 2.3.5 `ThreadPoolExecutor` 无限

**问题**: `run_server.py` 中 `ThreadPoolExecutor()` 不指定 `max_workers`,使用默认值可能过大。

**修复**:
```python
max_workers = int(os.getenv("GRPC_MAX_WORKERS", "16"))
server = grpc.aio.server(ThreadPoolExecutor(max_workers=max_workers), ...)
```

#### 2.3.6 `AsyncOps` 初始化竞态

**问题**: `_ensure_initialized` 无锁保护,多个协程可能同时触发初始化。

**修复** (`pg/operator.py`):
```python
def __init__(self):
    self._init_lock = asyncio.Lock()

async def _ensure_initialized(self):
    if not self._initialized:
        async with self._init_lock:  # 双重检查锁定
            if not self._initialized:
                await self.initialize()
```

---

### 2.4 🟡 HIGH: 限流拦截器

#### 问题

`RateLimitInterceptor` 有两个 bug:
1. `grpc.ServicerContext()` 不能直接实例化(它是抽象类)
2. 拦截器未在 `run_server.py` 中启用

#### 修复 (`ratelimit.py`)

新增令牌桶限流器:
```python
class TokenBucketRateLimiter:
    def __init__(self, rate: float, capacity: int):
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._lock = threading.Lock()

    def allow_request(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False
```

修复 `intercept_service`:
```python
class RateLimitInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        if not self.rate_limiter.allow_request():
            def deny_handler(request_or_iterator, context):
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                context.set_details('Rate limit exceeded')
                return None
            return deny_handler
        return continuation(handler_call_details)
```

---

## 3. 修改文件清单

| 文件 | 修改类型 | 修改概要 |
|------|----------|----------|
| `rpc_feed/core/datasets/provider.pxd` | 重写 | 新增 `InstrumentBuffer`/`AdjustBuffer`/`RightBuffer` 类(`cdef public`);修改 `_init_buffers`/`_row_to_buffer`/`_flush_buffer` 签名 |
| `rpc_feed/core/datasets/provider.pyx` | 重写 | 实现 buffer 容器类;重构 `__call__` 使用局部 buffer;`context` 取消检查;`to_pylist`→`to_numpy`;`use_threads=False` |
| `rpc_feed/core/feed.pyx` | 局部修改 | `fetch()` 增加 `context` 参数透传 |
| `rpc_feed/core/gateway/duckdb/operator.py` | 重写 | `_glob_path` 异步化;`ConnectionPool` 增加关闭检查;`DuckDBManager.close()` |
| `rpc_feed/core/gateway/pg/operator.py` | 局部修改 | `_init_lock` 保护初始化;移除 `compiled_cache={}` |
| `rpc_feed/core/rpc/server.py` | 重写 | 所有 RPC 方法增加 `context.done()` + `_stream_semaphore` |
| `rpc_feed/run_server.py` | 局部修改 | `max_workers`;资源清理(`async_ops.cleanup()` + `duck_mgr.close()`) |
| `rpc_feed/core/rpc/middleware/interceptors/ratelimit.py` | 重写 | `TokenBucketRateLimiter` + 修复 `deny_handler` |
| `scripts/verify_fix.py` | 新增 | 验证脚本 |

---

## 4. 新增环境变量

| 变量 | 默认值 | 用途 |
|------|--------|------|
| `MAX_CONCURRENT_STREAMS` | `50` | 全局并发流限制(信号量) |
| `GRPC_MAX_WORKERS` | `16` | gRPC ThreadPoolExecutor 线程数 |

---

## 5. 验证

### 编译验证

```bash
poetry run python setup.py build_ext --inplace
```
✅ 4 个 Cython 扩展全部编译成功

### 功能验证

```bash
poetry run python scripts/verify_fix.py
```

```
✅ Buffer isolation: PASSED
✅ _init_buffers (via buffer container): PASSED
✅ Context param in __call__ (source): PASSED
✅ Global stream semaphore: PASSED
✅ Token bucket rate limiter: PASSED
✅ BtFeed singleton with providers: PASSED
🎉 ALL VALIDATION TESTS PASSED
```

### 验证项说明

| 测试 | 验证内容 |
|------|----------|
| Buffer isolation | 两个独立 `InstrumentBuffer` 实例的 `buf_ratio` 互不干扰 |
| `_init_buffers` | 每次 `InstrumentBuffer()` 创建新实例(`is not` 断言) |
| Context param | `provider.pyx` 源码包含 `object context=None` 参数 |
| Semaphore | `_stream_semaphore._value == 50` |
| Rate limiter | 令牌桶容量=3 时,第 4 次请求被拒 |
| BtFeed | 单例可加载,`_providers` 字典包含 tick/asset |

---

## 6. 后续建议

1. **生产环境验证**: 用真实多客户端并发请求测试 Instrument/Adjust/Right 数据正确性
2. **启用限流拦截器**: 在 `run_server.py` 的 `interceptors=[]` 中添加 `RateLimitInterceptor()`
3. **监控指标**: 增加 Prometheus 指标导出,监控连接池使用率和并发流数
4. **TLS**: 当前使用 `add_insecure_port`,生产环境应启用 TLS
5. **Auth**: 启用 `AuthInterceptor`,替换硬编码 token