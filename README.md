# rpc-feed

面向中国 A 股/基金等金融行情数据的 **数据供给与 ETL 服务**。

- **数据供给层（Data Feed）**：通过 gRPC 流式接口对外提供历史行情数据（tick、daily、close、asset、adjust、right）
- **数据入湖管道（ETL Pipeline）**：将原始二进制 `.01` 文件或 CSV 经 DAG 处理后写入 Hive 分区的 Parquet 湖或 PostgreSQL

混合使用 **Cython**（性能关键路径）和 **Python**（编排层），通过 Poetry 管理依赖。

---

## 快速开始

```bash
# 1. 安装依赖
poetry install

# 2. 编译 Cython 扩展
poetry run python setup.py build_ext --inplace

# 3. 启动 gRPC 服务
poetry run python rpc_feed/run_server.py

# 4. 验证并发安全修复
poetry run python scripts/verify_fix.py
```

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 语言 | Python `>=3.11,<3.15` + Cython |
| RPC | gRPC + Protobuf（`bt-protocol` 包） |
| 数据 | PyArrow、DuckDB、Pandas、NumPy |
| 数据库 | PostgreSQL（SQLAlchemy + asyncpg） |
| DAG | NetworkX + loky ProcessPool |
| 异步 | uvloop + asyncio |
| 构建 | Poetry 声明依赖 + setuptools 编译 Cython |

---

## 项目结构

```
rpc_feed/
├── run_server.py              # gRPC 服务入口
├── core/
│   ├── datasets/              # 数据提供者（Cython 性能路径）
│   │   ├── provider.pyx/.pxd  # Tick/Daily/Close/Instrument/Adjust/Right + Buffer 容器
│   │   └── __init__.py        # _providers 注册表
│   ├── feed.pyx/.pxd          # BtFeed 单例：fetch() / load()
│   ├── gateway/
│   │   ├── duckdb/            # DuckDB 连接池 + Parquet 查询
│   │   └── pg/                # PostgreSQL 异步网关
│   ├── graph/                 # DAG ETL 引擎（loky + asyncio）
│   │   ├── to_graph.py        # 调度器
│   │   └── node/              # Loader/Format/Writer 节点
│   └── rpc/
│       ├── server.py          # gRPC 服务实现 + 并发控制
│       └── middleware/        # 限流/认证拦截器（备用）
├── utils/                     # 工具：日期转换、路径、单例装饰器
scripts/                       # ETL/运维脚本
xml/                           # GraphML DAG 配置
doc/                           # 文档
│   ├── optimize_plan.md       # 并发安全与性能修复方案
│   └── implementation_plan.md # 实施计划
```

> `rpc_feed/` 没有 `__init__.py`，是 Python 命名空间包。

---

## 架构设计

### gRPC 数据流

```
Client
  ↓ gRPC stream (ArrowFrame protobuf)
RpcServer (server.py)
  ↓ asyncio.Semaphore(50) 并发控制
  ↓ context.done() 客户端取消检测
bt_feed.fetch(topic, start, end, sids, context)
  ↓
Provider (单例)
  ↓ DuckDBManager.query()  ← Tick/Daily/Close
  ↓ AsyncOps.on_query()    ← Instrument/Adjust/Right
  ↓ 请求局部 Buffer 容器（无竞态）
ArrowFrame → Client
```

### 并发安全机制

| 机制 | 说明 |
|------|------|
| **请求局部 Buffer** | `InstrumentBuffer`/`AdjustBuffer`/`RightBuffer` 每次 `__call__` 创建新实例，避免单例 Provider 竞态 |
| **全局并发限制** | `asyncio.Semaphore(50)` 控制并发流数，防止单客户端耗尽连接池 |
| **客户端取消检测** | `context.done()` 检测客户端断开，提前退出释放 DuckDB/PG 连接 |
| **资源清理** | `shutdown()` 时调用 `async_ops.cleanup()` + `duck_mgr.close()` |
| **DuckDB 异步化** | `_glob_path` 通过 `run_in_executor` 避免阻塞事件循环 |
| **PG 初始化锁** | `asyncio.Lock` 双重检查，防止并发初始化 |

> 详细分析见 [`doc/optimize_plan.md`](doc/optimize_plan.md)

### DAG ETL 管道

```
xml/*.graphml → Graph._build_graph() → loky ProcessPool (Loader/Format)
    → asyncio Queue → Consumer (Writer) → Parquet/PostgreSQL/CSV
```

---

## RPC 接口

| RPC | topic | 数据源 | 说明 |
|-----|-------|--------|------|
| `TickStreamCall` | `tick` | DuckDB/Parquet | 逐笔行情 |
| `DailyStreamCall` | `daily` | DuckDB/Parquet | 日 K 线 |
| `CloseStreamCall` | `close` | DuckDB/Parquet | 收盘价 |
| `InstrumentCall` | `asset` | PostgreSQL | 标的元数据 |
| `AdjustmentStreamCall` | `adjust` | PostgreSQL | 除权除息 |
| `RightStreamCall` | `right` | PostgreSQL | 配股配息 |
| `CalendarCall` | `calendar` | — | ⚠️ 未实现 |

---

## 环境变量

### gRPC 服务

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `GRPC_SERVER` | — | 监听地址 |
| `MAX_MESSAGE_LENGTH` | `536870912` | 最大消息长度（512MB） |
| `MAX_CONCURRENT_STREAMS` | `50` | 全局并发流限制 |
| `GRPC_MAX_WORKERS` | `16` | ThreadPoolExecutor 线程数 |

### DuckDB

| 变量 | 说明 |
|------|------|
| `DUCKDATASET` | Parquet 数据根目录 |
| `DUCKDB` | DuckDB 缓存数据库文件名 |
| `DUCKBATCHSIZE` | 批次大小（默认 100000） |
| `DUCKCONNECTION` | 连接池大小（默认 10） |

### PostgreSQL

| 变量 | 说明 |
|------|------|
| `PGENGINE` / `PGUSER` / `PGPWD` | 引擎（asyncpg）/ 用户 / 密码 |
| `PGHOST` / `PGPORT` / `PGDB` | 主机 / 端口 / 数据库 |
| `PGPOOLSIZE` / `PGMAXOVERFLOW` | 连接池大小 / 溢出 |
| `PGPOOLRECYCLE` / `PGPREPING` | 回收时间 / 预检 |

### DAG ETL

| 变量 | 说明 |
|------|------|
| `CONCURRENT_PROCS` | loky 进程池大小 |
| `QUEUE_SIZE` | asyncio 队列大小 |

---

## Cython 编译

```bash
poetry run python setup.py build_ext --inplace
```

编译 4 个扩展模块：

| 模块 | 源文件 | 职责 |
|------|--------|------|
| `rpc_feed.core.datasets.provider` | `provider.pyx` | 数据提供者 + Buffer 容器 |
| `rpc_feed.core.feed` | `feed.pyx` | BtFeed 门面 |
| `rpc_feed.core.gateway.duckdb.utils` | `duckdb/utils.pyx` | 日期解析 + 分区展开 |
| `rpc_feed.utils.dateintern` | `dateintern.pyx` | C 级时间戳转换 |

编译选项：`-O3 -std=c++11`，Cython directives：`boundscheck=False`, `wraparound=False`, `cdivision=True`

> 修改 `.pyx`/`.pxd` 后必须重新编译。

---

## 开发指南

### 新增数据 topic

1. 在 `core/datasets/provider.pyx` 添加 Provider 类（继承 `BaseDuckDBProvider` 或 `BaseSQLAlchemyProvider`）
2. 若使用 SQLAlchemy，新增对应的 `XxxBuffer` 容器类（`cdef public` 属性）
3. 在 `core/datasets/__init__.py` 的 `_providers` 字典注册
4. 在 `core/rpc/server.py` 暴露 RPC 方法（使用 `async with _stream_semaphore` + `context.done()`）

### 新增 DAG 节点

1. 继承 `core/graph/node/node.py` 的 `Node`
2. 使用 `@registry` 装饰器注册
3. 在 `xml/*.graphml` 中配置节点和边

### 代码风格

- 中文注释和 docstring
- Cython 热路径使用 `cdef` 类与 `cdef` 函数
- Buffer 使用 NumPy 预分配，避免 `list.append` 的 `realloc`
- Arrow 路径优先 `batch.slice()` 零拷贝
- 配置通过 `.env` + `os.getenv`，不硬编码

---

## 验证

```bash
# 编译验证
poetry run python setup.py build_ext --inplace

# 功能验证（并发安全 / 限流 / semaphore）
poetry run python scripts/verify_fix.py
```

---

## 已知限制

- `CalendarCall` RPC 映射到未注册的 `"calendar"` provider
- gRPC 使用 `add_insecure_port`，未启用 TLS
- `AuthInterceptor` 存在但未启用，token 为硬编码 `"valid_token"`
- `tests/` 无可运行测试用例（`test_duckdb.py` import 路径已失效）
- Docker 基于 `python:3.9`（项目要求 `>=3.11`），且缺少 `init.sh`

---

## 相关文档

- [`doc/optimize_plan.md`](doc/optimize_plan.md) — 并发安全与性能修复方案（buffer 竞态分析、资源泄漏修复、性能优化）
- [`doc/implementation_plan.md`](doc/implementation_plan.md) — 实施计划文档