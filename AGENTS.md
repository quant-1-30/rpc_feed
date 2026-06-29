## 1. 项目概述

`rpc-feed` 是一个面向中国 A 股/基金等金融行情数据的 **数据供给与 ETL 服务**。

它承担两个核心职责：

1. **数据供给层（Data Feed）**：通过 gRPC 流式接口对外提供历史行情数据（tick、daily、close、asset、adjust、right）。
2. **数据入湖管道（ETL Pipeline）**：把原始二进制 `.01` 文件或 CSV 经过可配置的 DAG 处理后写入 Hive 分区的 Parquet 湖，或写入 PostgreSQL。

项目大量混合使用 **Cython**（性能关键路径）和 **普通 Python**（编排层），并通过 Poetry 管理依赖。

---

## 2. 技术栈

- **Python**：`>=3.11,<3.15`（当前环境为 3.11）
- **依赖管理**：Poetry 1.8+
- **构建后端**：setuptools + `setup.py`（非 Poetry 的 build-backend）
- **Cython**：`.pyx`/`.pxd` 编译为 C++ 扩展，开启 `-O3 -std=c++11`
- **RPC**：gRPC + Protobuf，通过 `bt-protocol` 包提供 service/schema/template
- **数据/Arrow**：PyArrow、DuckDB、Pandas、Polars、NumPy
- **数据库**：PostgreSQL（SQLAlchemy + asyncpg + psycopg）
- **DAG/并行**：NetworkX、loky、joblib
- **Avro**：avro
- **可视化/调试**：pyvis、matplotlib
- **其他**：uvloop、python-dotenv、psutil、pyyaml、openpyxl、networkx

> 注意：`pyproject.toml` 使用 Poetry 依赖声明，但 `[build-system]` 明确指定 `setuptools.build_meta`，Cython 扩展由 `setup.py` 构建。

---

## 3. 项目结构

```
rpc_feed/
├── errors.py                 # 自定义异常：NoDataOnDate / NoDataForSid 等
├── run_server.py             # gRPC 服务入口（uvloop + asyncio）
├── core/
│   ├── datasets/             # Cython 数据提供者（核心性能路径）
│   │   ├── provider.pyx/.pxd # Tick / Daily / Close / Instrument / Adjust / Right
│   │   └── __init__.py       # 注册 _providers 字典
│   ├── feed.pyx/.pxd         # BtFeed 单例门面：fetch(...) / load(...)
│   ├── gateway/              # 数据库/存储适配层
│   │   ├── duckdb/           # DuckDB + Parquet 湖查询
│   │   └── pg/               # PostgreSQL 异步网关
│   ├── graph/                # DAG ETL 引擎
│   │   ├── to_graph.py       # Graph 单例：调度、拓扑排序、loky 进程池
│   │   ├── node/             # Loader / Format / Abnormal / Writer 节点
│   │   ├── serialize.py      # loky worker 序列化/反序列化
│   │   ├── monitor.py        # 内存监控与 GC
│   │   ├── meta.py           # 声明式 params 元类框架
│   │   └── visualize.py      # Graphviz 可视化辅助
│   └── rpc/
│       ├── server.py         # btDataFeed gRPC 服务实现
│       └── middleware/       # Auth/RateLimit 拦截器（当前未启用）
└── utils/
    ├── context_tricks.py     # warning/临时目录上下文
    ├── dateintern.pyx/.pxd   # Cython 高性能日期时间转换
    ├── io.py                 # 路径、glob、配置解析、build_from_cfg
    ├── loader.py             # 动态模块加载
    ├── weak_ref.py           # 弱引用 LRU 缓存
    └── wrapper.py            # 单例、注册表、装饰器

scripts/                      # 运维/ETL 脚本
├── asset_writer.py           # asset 元数据导入 PG
├── partition.py              # tick 表按季度分区（当前有 import 错误）
├── pipeline.py               # DAG 管道执行入口
├── rewrite_parquet.py        # 离线修正 parquet 时区/tick
├── run.sh                    # 服务启动脚本
└── sync.py                   # 增量 parquet 合并压缩

tests/                        # 测试/实验文件
├── provider.pxd              # 旧版/独立 Cython 声明
├── provider.pyx              # 旧版/独立 Cython 实现（未被 setup.py 编译）
└── test_duckdb.py            # 手动脚本（import 路径已失效）

xml/                          # GraphML 格式 DAG 配置
├── tick.graphml / tick_csv.graphml
├── fund.graphml / fund_csv.graphml
├── benchmark_csv.graphml
├── asset.graphml
└── test.graphml
```

> 细节：`rpc_feed/` 目录下**没有** `__init__.py`，因此它是一个 Python **命名空间包（namespace package）**。`python -c "import rpc_feed; print(rpc_feed.__file__)"` 会输出 `None`。

---

## 4. 构建与安装

### 4.1 安装依赖

项目使用 Poetry 虚拟环境：

```bash
poetry install
```

国内环境依赖 `pyproject.toml` 中已配置清华 tuna 源：

```toml
[[tool.poetry.source]]
name = "tuna"
url = "https://pypi.tuna.tsinghua.edu.cn/simple/"
priority = "primary"
```

另有一个 supplemental 源 `devpi` 指向 `http://localhost:3141/bt_sdk/dev/+simple/`，用于获取 `bt-protocol` 等私有包；本地未启动 devpi 时可能无法解析。

### 4.2 编译 Cython 扩展

```bash
poetry run python setup.py build_ext --inplace
```

`setup.py` 会编译以下 4 个扩展：

| 扩展模块 | 源文件 |
|----------|--------|
| `rpc_feed.core.datasets.provider` | `rpc_feed/core/datasets/provider.pyx` |
| `rpc_feed.core.feed` | `rpc_feed/core/feed.pyx` |
| `rpc_feed.core.gateway.duckdb.utils` | `rpc_feed/core/gateway/duckdb/utils.pyx` |
| `rpc_feed.utils.dateintern` | `rpc_feed/utils/dateintern.pyx` |

编译选项：

- `-O3 -std=c++11`
- `-Wno-unreachable-code`
- Cython compiler directives：`boundscheck=False`, `wraparound=False`, `initializedcheck=False`, `cdivision=True`

> 注意：`tests/provider.pyx` **不在** `setup.py` 的构建列表中，仅作为历史/参考文件存在。

### 4.3 清理构建产物

`.gitignore` 排除了 `*.cpp`、`*.so`、`build/` 等，但仓库中目前仍提交了编译产物。需要时可手动清理：

```bash
find rpc_feed tests -name "*.cpp" -o -name "*.so" | xargs rm -f
rm -rf build/
```

---

## 5. 测试


- `tests/test_duckdb.py` 导入 `rpc_feed.core.operator`，该模块已不存在，pytest 收集阶段直接报错。

### 5.1 当前可执行的“验证”

```bash
# 确认 Cython 扩展能编译
poetry run python setup.py build_ext --inplace

# 确认基本导入
poetry run python -c "from rpc_feed.core.feed import bt_feed; print(bt_feed)"
```

### 5.2 运行 pytest

```bash
poetry run pytest tests/ -v
```

目前会因 `tests/test_duckdb.py` 的 import 错误而失败。需要修复测试或将其排除后才能通过收集阶段。

---

## 6. 运行架构

### 6.1 gRPC 数据服务

入口：`rpc_feed/run_server.py`

```bash
poetry run python rpc_feed/run_server.py
```

流程：

```
run_server.py
    ↓
grpc.aio.server (uvloop + ThreadPoolExecutor)
    ↓
RpcServer (core/rpc/server.py)
    ↓
bt_feed.fetch(topic, start_date, end_date, sids)
    ↓
core/datasets/_providers[topic]
    ↓
DuckDBManager 或 async_ops (gateway)
    ↓
ArrowFrame (protobuf) 流式返回客户端
```

支持的 RPC 方法：

| RPC | topic | 数据源 |
|-----|-------|--------|
| `InstrumentCall` | `asset` | PostgreSQL `Asset` 表 |
| `TickStreamCall` | `tick` | DuckDB/Parquet |
| `DailyStreamCall` | `daily` | DuckDB/Parquet |
| `CloseStreamCall` | `close` | DuckDB/Parquet |
| `AdjustmentStreamCall` | `adjust` | PostgreSQL `Adjustment` 表 |
| `RightStreamCall` | `right` | PostgreSQL `Rightment` 表 |
| `CalendarCall` | `calendar` | ⚠️ 未注册提供者 |

### 6.2 DAG ETL 管道

入口：`scripts/pipeline.py` 或直接调用 `bt_feed.load(...)`

流程：

```
xml/*.graphml
    ↓
Graph._build_graph() (networkx + topological_sort)
    ↓
loky ProcessPool 执行 Loader/Format/Abnormal 节点
    ↓
asyncio Queue + Consumer 执行 Writer 节点
    ↓
Parquet / PostgreSQL / CSV
```

典型 GraphML 流程：

```
StructUnpacker → StructDateParser → Multiply → Dtypes → ParquetWriter
TextLoader     → UniverseDateParser → Multiply → Dtypes → ParquetWriter
TextLoader     → PgWriter   (asset.graphml)
```

### 6.3 关键环境变量

运行时大量依赖 `.env` 中的变量（`.env` 文件含敏感信息，ReadFile 被系统保护，无法在此读取）：

| 变量 | 用途 |
|------|------|
| `GRPC_SERVER` | gRPC 监听地址 |
| `MAX_MESSAGE_LENGTH` | 最大消息长度，默认 512MB |
| `DUCKDATASET` / `DUCKDB` / `DUCKBATCHSIZE` / `DUCKCONNECTION` | DuckDB 数据路径与连接 |
| `PGENGINE` / `PGUSER` / `PGPWD` / `PGHOST` / `PGPORT` / `PGDB` | PostgreSQL 连接 |
| `CONCURRENT_PROCS` | DAG 进程池大小 |
| `QUEUE_SIZE` | DAG 队列大小 |

---

## 7. 模块划分与职责

### 7.1 `core/datasets/provider.pyx` — 数据提供者

- `BaseBufferedProvider`：共享的 Arrow 帧构造辅助。
- `BaseDuckDBProvider` / `BaseSQLAlchemyProvider`：分别对应 DuckDB 与 PostgreSQL 查询。
- `Tick` / `Daily` / `Close`：DuckDB 源，按 `sid` 切片输出 ArrowFrame。
- `Instrument` / `Adjust` / `Right`：PostgreSQL 源，预分配 `CHUNK_SIZE=1024` 的 NumPy 缓冲区。

### 7.2 `core/gateway/` — 存储适配

- `duckdb/operator.py`：`ConnectionPool` + `DuckDBManager` 单例，负责把请求映射到 Hive 分区路径并执行模板 SQL。
- `duckdb/utils.pyx`：`Request` C 结构体、日期解析、分区范围展开（`schema_range`）。
- `pg/operator.py`：`AsyncOps` 单例，基于 SQLAlchemy async engine + `AsyncSession`。

### 7.3 `core/graph/` — DAG 引擎

- `to_graph.py`：核心调度器，混合 `loky` 进程池与 `asyncio`。
- `node/`：节点实现
  - `loader.py`：`StructUnpacker`、`AvroUnpacker`、`TextLoader`
  - `format.py`：`StructDateParser`、`UniverseDateParser`、`Multiply`、`Dtypes`
  - `abnormal.py`：`ProcessNa`、`ProcessInf`
  - `writer.py`：`AvroWriter`、`CsvWriter`、`ParquetWriter`、`PgWriter`
- `meta.py`：声明式 `params` 元类（类似 backtrader/zipline 风格）。
- `serialize.py`：worker 进程重建 pipeline 的序列化逻辑。

### 7.4 `core/rpc/server.py` — gRPC 服务

实现 `btDataFeedServicer`，把每个 RPC 映射到 `bt_feed.fetch(...)`。

### 7.5 `core/feed.pyx` — 门面

`BtFeed` 单例：

- `_providers`：来自 `datasets/__init__.py` 的注册表。
- `pipeline`：`Graph()` 单例。
- `fetch(topic, ...)`：异步流式取数。
- `load(xml, dataset_path, prefix, parallel)`：执行 DAG 管道。

### 7.6 `utils/` — 通用工具

- `dateintern.pyx`：C 级时间戳/日期转换，固定上海市场开盘/收盘偏移。
- `io.py`：`build_from_cfg`（通过注册表反射构造节点）、`recursive_glob`、路径工具。
- `wrapper.py`：`@singleton`、`@registry`、通用装饰器。
- `weak_ref.py`：弱引用 LRU 缓存。
- `loader.py`：动态模块加载。
- `context_tricks.py`：warning 上下文（当前存在重复定义与缺失 import）。

---

## 8. 代码风格与约定

- **语言**：中文注释和 docstring 占主导；提交 AGENTS.md 等文档时请使用中文。
- **编码声明**：大多数 Python 文件顶部保留 `# -*- coding: utf-8 -*-`。
- **Cython 性能风格**：
  - 热路径使用 `cdef` 类与 `cdef` 函数。
  - 通过 `__cinit__` / `__init__` 分工：C 成员在 `__cinit__` 初始化，Python 成员在 `__init__` 初始化。
  - 使用 NumPy 预分配缓冲区避免 `list.append` 的 `realloc`。
  - Arrow 路径优先使用 `batch.slice()` 零拷贝。
- **节点注册**：DAG 节点通过 `@registry` 注册，类名最后一个下划线后的 token 作为注册名（如 `PgWriter` 注册为 `"Writer"`）。
- **单例模式**：`@singleton` 装饰器用于 `AsyncOps`、`Graph`、`DuckDBManager` 等。
- **环境驱动**：配置优先通过 `.env` + `os.getenv` 读取，而非硬编码。

---

## 9. 部署

### 9.1 本地/开发启动

```bash
poetry install
poetry run python setup.py build_ext --inplace
poetry run python rpc_feed/run_server.py
```

### 9.2 脚本部署

`scripts/run.sh` 意图用于生产启动：

- 设置 `PYTHONPATH=$PWD`
- 创建 `/var/log/rpc_feed.*.log`
- 安装 Poetry
- 运行 `script/pg_init.py`（⚠️ 路径应为 `scripts/pg_init.py`，且仓库中当前不存在 `pg_init.py`）
- 启动 `rpc_feed/run_server.py`

### 9.3 Docker

`Dockerfile` 基于 `python:3.9`（注意项目要求 Python `>=3.11`），使用 Poetry 安装，并依赖一个本地 whl 文件 `poetry-1.7.1-py3-none-any.whl`。镜像启动命令为 `bash ./init.sh`，但仓库中未包含 `init.sh`。

---

## 10. 已知问题与注意事项（基于实际代码）

以下问题来自对源码的直接检查，不是假设：

### 10.1 无法运行的测试/脚本

- `tests/test_duckdb.py`：导入 `rpc_feed.core.operator`，该模块不存在。
- `scripts/partition.py`：缺失 `Any`/`datetime`/`pytz` import，且导入不存在的 `.pg_init` 和 `rpc_feed.core.operator.async_ops`。
- `scripts/pipeline.py`：导入 `rpc_feed.core.rpc.feed`，实际路径为 `rpc_feed.core.feed`。
- `scripts/run.sh`：引用 `script/pg_init.py`，目录名应为 `scripts/`；且 `pg_init.py` 不存在。

### 10.2 功能缺口 / 潜在 Bug

- `CalendarCall` RPC 映射到未注册的 `"calendar"` provider。
- `TextLoader.prenext()` 中 `f.readlines` 缺少 `()`。
- `AvroWriter.next()` 语法错误：`open(self.p.data_path), "wb"` 括号不匹配；`dicts.values` 缺少 `()`。
- `ParquetWriter.next()` 是 `async def`，但直接调用同步 `_write_parquet()`，未使用 `run_in_executor`/`to_thread`。
- `PgWriter` 支持 `update`/`delete` 模式，但 `AsyncOps` 只有 `on_insert`/`on_query`，没有 `on_update`/`on_delete`。
- `context_tricks.py` 中 `ignore_pandas_nan_categorical_warning` 被定义两次，第二次缺少 `warnings`/`mkdtemp`/`shutil` import；`get_temp_dir` 也缺少这些 import。
- `utils/io.py` 中 `read_bin` 对字符串调用 `file_path.expanduser()`，应先用 `Path(...)` 包裹。
- `utils/loader.py` 中 `import_file` 引用未定义的 `CUSTOM_LOADED_MODULES`；`load_extensions` 调用不存在的 `np.concatv`。
- `utils/wrapper.py` 中 `api_method` 引用未定义的 `get_algo_instance`；`coerce_numbers_to_my_dtype` 引用未定义的 `Number`/`coerce_to_dtype`。
- `core/rpc/middleware/` 下有 `AuthInterceptor` / `RateLimitInterceptor`，但 `run_server.py` 传给 gRPC server 的 `interceptors=[]` 为空列表，拦截器未生效。
- `AuthInterceptor` 使用硬编码 token `"valid_token"`。

### 10.3 代码与路径硬编码

多个脚本保留本地开发路径（如 `/Users/hengxinliu/Downloads/...`），这些属于临时/一次性工具，尚未产品化。

### 10.4 构建产物提交

`.gitignore` 已排除 `*.cpp` / `*.so` / `build/`，但仓库工作区中仍包含这些编译产物，可能是误提交。

---

## 11. 安全注意事项

- `.env` 文件包含数据库连接等敏感信息，已被系统标记为敏感文件。不要把它提交到 Git，也不要在日志中打印。
- gRPC 当前使用 `add_insecure_port`，未启用 TLS。
- `AuthInterceptor` 虽然存在但未被启用，且 token 为硬编码 `"valid_token"`，不能用于生产鉴权。
- `.gitignore` 已忽略 `.env`，请保持这一约定。
- Cython 编译选项关闭了 `boundscheck`/`wraparound`/`initializedcheck` 并开启 `cdivision=True`，存在段错误风险；修改 Cython 代码后必须重新编译并做边界测试。

---

## 12. 给 AI 助手的快速备忘

- 修改 `.pyx`/`.pxd` 后务必运行 `poetry run python setup.py build_ext --inplace`。
- 新增数据 topic：在 `core/datasets/provider.pyx` 添加 provider 类，在 `core/datasets/__init__.py` 注册，并在 `core/rpc/server.py` 暴露 RPC。
- 新增 DAG 节点：继承 `core/graph/node/node.py` 的 `Node`，使用 `@registry`，在 GraphML 中配置。
- 不要依赖 `tests/` 作为回归测试基线；当前它没有可运行用例。
- 修改 `scripts/` 或 `utils/` 后，先手动 `poetry run python -c "import ..."` 检查 import 错误。
- 所有文档/注释以中文为主。
