# Implementation Plan

[Overview]
全面修复 rpc_feed gRPC 数据服务端在多客户端并发场景下的内存泄漏和性能卡点问题。

rpc_feed 作为面向多客户端的 gRPC 流式数据服务,当前代码存在三类严重问题:(1) 并发安全 — SQLAlchemy Provider 的 numpy buffer 作为单例实例属性在并发请求间被互相覆盖,导致数据错乱;(2) 资源泄漏 — 缺少客户端取消处理,DuckDB 连接和 PG session 在客户端断开后无法释放;(3) 性能卡点 — `_glob_path` 的同步 `os.path.exists()` 阻塞事件循环,`compiled_cache={}` 禁用 SQL 缓存,`to_pylist()` 创建大量 Python 对象。本次修复覆盖从 Cython 底层(`.pyx`/`.pxd`)到 Python 编排层(`server.py`/`operator.py`)的全链路,确保服务在高并发下稳定运行。

[Types]

本次修改涉及的类型系统变更,主要是新增请求局部 buffer 容器类和并发控制原语。

```cython
# provider.pxd — 新增 buffer 容器类(请求局部,替代实例属性)

cdef class InstrumentBuffer:
    """Instrument Provider 的请求局部 buffer,每次 __call__ 创建新实例"""
    cdef list buf_sid
    cdef list buf_name
    cdef object buf_first_trading
    cdef object buf_delist
    cdef list buf_merger
    cdef object buf_ratio

cdef class AdjustBuffer:
    """Adjust Provider 的请求局部 buffer"""
    cdef object buf_ex_date
    cdef object buf_register_date
    cdef object buf_bonus_share
    cdef object buf_transfer
    cdef object buf_bonus

cdef class RightBuffer:
    """Right Provider 的请求局部 buffer"""
    cdef object buf_ex_date
    cdef object buf_register_date
    cdef object buf_price
    cdef object buf_ratio
```

```python
# server.py — 新增的并发控制数据结构

# 信号量限流(替代 interceptor,更简单可靠)
_MAX_CONCURRENT_STREAMS = int(os.getenv("MAX_CONCURRENT_STREAMS", "50"))
_global_stream_semaphore: asyncio.Semaphore  # 在 serve() 中初始化
```

```python
# ratelimit.py — 重写的限流器

class TokenBucketRateLimiter:
    """令牌桶限流器,线程安全"""
    def __init__(self, rate: float, capacity: int): ...
    def allow_request(self) -> bool: ...
```

[Files]

本次修改涉及 9 个文件,分为 3 个修复阶段。

### 需要修改的文件

| 文件 | 阶段 | 修改概要 |
|------|------|----------|
| `rpc_feed/core/datasets/provider.pxd` | P1 | 移除 Provider 子类的实例 buffer 属性声明;新增 `InstrumentBuffer`/`AdjustBuffer`/`RightBuffer` 容器类;修改 `_init_buffers`/`_row_to_buffer`/`_flush_buffer` 签名 |
| `rpc_feed/core/datasets/provider.pyx` | P1 | buffer 改为 `__call__` 局部变量;`__call__` 增加取消检查;`_process_batch` 优化 `to_pylist` → `to_numpy`;`batch_to_resp` 的 `use_threads` 设为 `False` |
| `rpc_feed/core/gateway/pg/operator.py` | P1 | `AsyncOps` 增加 `asyncio.Lock` 保护初始化;移除 `compiled_cache={}` 改用默认缓存;增加 `cleanup()` 在关闭时 dispose engine |
| `rpc_feed/core/rpc/server.py` | P1 | 每个 RPC 方法增加 `context.is_active()` 检查;增加 `asyncio.Semaphore` 全局并发限制;增加 `finally` 清理 |
| `rpc_feed/run_server.py` | P1 | `ThreadPoolExecutor` 增加 `max_workers`;增加 PG/DuckDB 关闭时的 `await cleanup()`;增加 `async_ops.cleanup()` 调用 |
| `rpc_feed/core/gateway/duckdb/operator.py` | P2 | `_glob_path` 移至 `run_in_executor`;`get_connection` 改为非阻塞异步获取;`DuckDBManager` 增加 `close()` 方法;移除冗余 `get_duckdb_manager()` 单例锁 |
| `rpc_feed/core/rpc/middleware/interceptors/ratelimit.py` | P3 | 重写限流器;修复 `grpc.ServicerContext()` 实例化错误 |
| `rpc_feed/core/rpc/middleware/interceptors/auth.py` | P3 | 修复拦截器上下文处理 |
| `rpc_feed/core/feed.pyx` | P1 | `fetch()` 增加 `context` 参数透传(可选,用于取消检查) |

### 不需要修改的文件

- `rpc_feed/core/feed.pxd` — 无需改动(fetch 签名保持兼容)
- `rpc_feed/core/gateway/__init__.py` — 导出不变
- `rpc_feed/core/datasets/__init__.py` — Provider 单例注册不变(Mode B 保持单例)
- `rpc_feed/utils/wrapper.py` — 单例装饰器不变

[Functions]

新增和修改的函数清单。

### 新增函数

| 函数 | 文件 | 签名 | 用途 |
|------|------|------|------|
| `_check_cancelled` | `server.py` | `(context) -> None` | 检查 gRPC context 是否已取消,若取消则抛出 CancelledError |
| `TokenBucketRateLimiter.__init__` | `ratelimit.py` | `(rate: float, capacity: int)` | 令牌桶限流器初始化 |
| `TokenBucketRateLimiter.allow_request` | `ratelimit.py` | `() -> bool` | 尝试获取令牌 |
| `DuckDBManager.close` | `duckdb/operator.py` | `() -> None` | 关闭所有连接池连接 |
| `DuckDBManager._glob_path_async` | `duckdb/operator.py` | `async (req: dict) -> list` | 异步执行文件路径 glob |

### 修改函数

| 函数 | 文件 | 当前签名/行为 | 修改内容 |
|------|------|---------------|----------|
| `BaseSQLAlchemyProvider._init_buffers` | `provider.pyx` | `cdef void _init_buffers(self)` | 改为 `cdef object _init_buffers(self)` 返回 buffer 对象;不再写 `self.buf_*` |
| `BaseSQLAlchemyProvider._row_to_buffer` | `provider.pyx` | `cdef void _row_to_buffer(self, int i, object row)` | 增加 `object buf` 参数:`_row_to_buffer(self, buf, int i, object row)` |
| `BaseSQLAlchemyProvider._flush_buffer` | `provider.pyx` | `cdef object _flush_buffer(self, int count, bytes sid)` | 增加 `object buf` 参数:`_flush_buffer(self, buf, int count, bytes sid)` |
| `BaseSQLAlchemyProvider.__call__` | `provider.pyx` | 局部变量 `i`, `row` 等 | 增加局部 `buf = self._init_buffers()`;`_init_buffers`/`_row_to_buffer`/`_flush_buffer` 调用传入 `buf`;增加 `context` 取消检查 |
| `BaseDuckDBProvider.__call__` | `provider.pyx` | `async def __call__(...)` | 增加 `context` 参数;在 `async for` 循环中检查取消状态 |
| `_process_batch` | `provider.pyx` | `to_pylist()` 三次 | `s_indices`/`e_indices` 改用 `.to_numpy(zero_copy_only=False)`;`sid_col` 按边界索引取值 |
| `batch_to_resp` | `provider.pyx` | `IpcWriteOptions(use_threads=True)` | `use_threads` 改为 `False` 避免高并发线程竞争 |
| `Instrument._init_buffers` | `provider.pyx` | 写 `self.buf_*` | 返回 `InstrumentBuffer()` 实例并填充 |
| `Instrument._row_to_buffer` | `provider.pyx` | 写 `self.buf_*[i]` | 写 `buf.buf_*[i]` |
| `Instrument._flush_buffer` | `provider.pyx` | 读 `self.buf_*[:count]` | 读 `buf.buf_*[:count]` |
| `Adjust._init_buffers` | `provider.pyx` | 同上 | 返回 `AdjustBuffer()` |
| `Adjust._row_to_buffer` | `provider.pyx` | 同上 | 同上 |
| `Adjust._flush_buffer` | `provider.pyx` | 同上 | 同上 |
| `Right._init_buffers` | `provider.pyx` | 同上 | 返回 `RightBuffer()` |
| `Right._row_to_buffer` | `provider.pyx` | 同上 | 同上 |
| `Right._flush_buffer` | `provider.pyx` | 同上 | 同上 |
| `DuckDBManager.query` | `duckdb/operator.py` | 同步 `_glob_path` | `_glob_path` 改为 `await loop.run_in_executor(None, self._glob_path, req)` |
| `DuckDBManager.query` | `duckdb/operator.py` | 同步 `get_connection` | 增加超时回退逻辑;连接获取失败时返回而非抛异常 |
| `AsyncOps._ensure_initialized` | `pg/operator.py` | 无锁保护 | 增加 `asyncio.Lock` 防止并发初始化 |
| `AsyncOps.initialize` | `pg/operator.py` | `compiled_cache={}` | 移除 `compiled_cache={}`,使用默认 SQLAlchemy 缓存 |
| `RpcServer.TickStreamCall` | `server.py` | 无取消检查 | 在 `async for` 中增加 `if not context.is_active(): return` |
| `RpcServer.DailyStreamCall` | `server.py` | 同上 | 同上 |
| `RpcServer.CloseStreamCall` | `server.py` | 同上 | 同上 |
| `RpcServer.InstrumentCall` | `server.py` | 同上 | 同上 |
| `RpcServer.AdjustmentStreamCall` | `server.py` | 同上 | 同上 |
| `RpcServer.RightStreamCall` | `server.py` | 同上 | 同上 |
| `serve` | `run_server.py` | `ThreadPoolExecutor()` 无参数 | 增加 `max_workers=int(os.getenv("GRPC_MAX_WORKERS", "16"))` |
| `serve` | `run_server.py` | `interceptors=[]` | 启用限流拦截器 |
| `shutdown` | `run_server.py` | 无资源清理 | 增加 `await async_ops.cleanup()` 和 DuckDB 连接池关闭 |
| `RateLimitInterceptor.intercept_service` | `ratelimit.py` | `grpc.ServicerContext()` 实例化错误 | 修复为正确拒绝请求的方式 |
| `BtFeed.fetch` | `feed.pyx` | 无 context 参数 | 增加可选 `object context=None` 参数,透传给 provider 的 `__call__` |

### 移除函数

| 函数 | 文件 | 原因 |
|------|------|------|
| `get_duckdb_manager` | `duckdb/operator.py` | `DuckDBManager` 已是 `@singleton`,可直接通过 `DuckDBManager()` 获取;`_duck_inst`/`_duck_lock` 全局变量冗余 |

**迁移策略**: `gateway/__init__.py` 中的 `from .duckdb.operator import get_duckdb_manager` 改为 `from .duckdb.operator import DuckDBManager`;`provider.pyx` 中 `get_duckdb_manager()` 调用改为 `DuckDBManager()`。

[Classes]

类的修改清单。

### 修改的类

| 类 | 文件 | 修改内容 |
|----|------|----------|
| `InstrumentBuffer` (新增) | `provider.pxd`/`.pyx` | 请求局部 buffer 容器,持有 `buf_sid`/`buf_name`/`buf_first_trading`/`buf_delist`/`buf_merger`/`buf_ratio` |
| `AdjustBuffer` (新增) | `provider.pxd`/`.pyx` | 请求局部 buffer 容器,持有 `buf_ex_date`/`buf_register_date`/`buf_bonus_share`/`buf_transfer`/`buf_bonus` |
| `RightBuffer` (新增) | `provider.pxd`/`.pyx` | 请求局部 buffer 容器,持有 `buf_ex_date`/`buf_register_date`/`buf_price`/`buf_ratio` |
| `BaseSQLAlchemyProvider` | `provider.pxd`/`.pyx` | `_init_buffers` 返回值从 `void` 改为 `object`;`_row_to_buffer`/`_flush_buffer` 增加 `buf` 参数;`__call__` 使用局部 buffer |
| `Instrument` | `provider.pxd`/`.pyx` | 移除 `cdef list buf_sid` 等实例属性声明;`_init_buffers` 返回 `InstrumentBuffer` |
| `Adjust` | `provider.pxd`/`.pyx` | 移除 `cdef object buf_ex_date` 等实例属性声明;`_init_buffers` 返回 `AdjustBuffer` |
| `Right` | `provider.pxd`/`.pyx` | 移除 `cdef object buf_ex_date` 等实例属性声明;`_init_buffers` 返回 `RightBuffer` |
| `BaseDuckDBProvider` | `provider.pyx` | `__call__` 增加取消检查;`_process_batch` 优化内存 |
| `DuckDBManager` | `duckdb/operator.py` | 增加 `close()` 方法;`query` 中 `_glob_path` 异步化;增加 `__aexit__` 资源保护 |
| `ConnectionPool` | `duckdb/operator.py` | `get_connection` 增加日志;`close_all` 确保线程安全 |
| `AsyncOps` | `pg/operator.py` | 增加 `_init_lock: asyncio.Lock`;移除 `compiled_cache={}`;增加 `cleanup` 的连接池关闭 |
| `RpcServer` | `server.py` | 所有 RPC 方法增加取消检查和 semaphore |
| `TokenBucketRateLimiter` (新增) | `ratelimit.py` | 令牌桶限流,替代当前损坏的拦截器 |
| `RateLimitInterceptor` | `ratelimit.py` | 重写 `intercept_service`,修复上下文错误 |

### 不修改的类

- `BtFeed` (feed.pyx) — 保持单例,仅 `fetch` 签名微调
- `Graph` (to_graph.py) — DAG 引擎与 RPC 服务无直接关联
- `ConnectionPool` 的基本结构不变,仅增加日志

[Dependencies]

无新增第三方包依赖。所有修复使用现有依赖:

| 包 | 用途 | 版本要求 |
|----|------|----------|
| `grpcio` | gRPC 服务端(已安装) | 无版本变更 |
| `pyarrow` | Arrow 批处理(已安装 `^20.0.0`) | `to_numpy()` 需 `>=14.0`,已满足 |
| `duckdb` | DuckDB 查询(已安装 `^1.3.0`) | 无版本变更 |
| `sqlalchemy` | PG 异步(已安装,通过 bt-protocol 传递) | 无版本变更 |

**注意**: 修改 `.pyx`/`.pxd` 后必须运行:
```bash
poetry run python setup.py build_ext --inplace
```

[Testing]

当前项目无可用测试基线(`tests/test_duckdb.py` 因 import 错误无法运行)。本次修复采用以下验证策略:

### 验证步骤

1. **编译验证**:
   ```bash
   poetry run python setup.py build_ext --inplace 2>&1 | tail -5
   ```
   确认无编译错误。

2. **导入验证**:
   ```bash
   poetry run python -c "from rpc_feed.core.feed import bt_feed; print(bt_feed)"
   poetry run python -c "from rpc_feed.core.datasets.provider import InstrumentBuffer, AdjustBuffer, RightBuffer; print('OK')"
   poetry run python -c "from rpc_feed.core.gateway import async_ops, get_duckdb_manager; print('OK')"
   ```

3. **并发安全验证(手动)**:
   ```bash
   # 启动服务后,用两个客户端同时请求 Instrument 数据,验证返回的数据是否各自正确
   poetry run python rpc_feed/run_server.py
   ```

4. **取消处理验证**:
   - 客户端发起 stream 请求后立即断开,观察服务端日志是否打印取消信息且无连接泄漏。

5. **性能对比**:
   - 修复前后用相同参数请求 tick 数据,对比 RSS 内存和响应延迟。

[Implementation Order]

按以下顺序实施,每步完成后验证编译和导入:

1. **修改 `provider.pxd`** — 新增 `InstrumentBuffer`/`AdjustBuffer`/`RightBuffer` 类声明;移除 `Instrument`/`Adjust`/`Right` 的实例 buffer 属性;修改 `_init_buffers`/`_row_to_buffer`/`_flush_buffer` 签名

2. **修改 `provider.pyx`** — 实现 buffer 容器类;重构 `BaseSQLAlchemyProvider.__call__` 使用局部 buffer;修改各子类的 `_init_buffers`/`_row_to_buffer`/`_flush_buffer`;优化 `_process_batch` 的 `to_pylist`;`batch_to_resp` 的 `use_threads=False`;增加取消检查

3. **修改 `feed.pyx`** — `fetch` 增加可选 `context` 参数透传

4. **修改 `duckdb/operator.py`** — `_glob_path` 异步化;增加 `close()` 方法;移除冗余 `get_duckdb_manager`;`query` 增加连接获取失败处理

5. **修改 `pg/operator.py`** — 增加 `_init_lock`;移除 `compiled_cache={}`;修复初始化竞态

6. **修改 `server.py`** — 所有 RPC 方法增加 `context.is_active()` 检查和 semaphore

7. **修改 `run_server.py`** — `ThreadPoolExecutor` 增加 `max_workers`;启用拦截器;`shutdown` 增加资源清理

8. **修改 `ratelimit.py`** — 重写限流器,修复 `intercept_service`

9. **修改 `gateway/__init__.py`** — 更新导入(如移除 `get_duckdb_manager`)

10. **编译验证** — `poetry run python setup.py build_ext --inplace`

11. **导入验证** — 确认所有模块可正常导入