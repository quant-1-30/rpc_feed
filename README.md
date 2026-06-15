# cython 重构 providers
# cython 重构 model and  util
# cython 重构 rpc server

# arrow string / bytes
<!-- buffers[0] → validity bitmap
buffers[1] → offsets (int32 / int64)
buffers[2] → data (byte blob) -->

# Arrow 格式**（如 DuckDB、Parquet 文件、Pandas DataFrame）时，转为 PyArrow 才有性能优势，因为那时是 **Zero-Copy**

compiled = compile(
    exec_str,
    func.__code__.co_filename,
    mode='exec',
)
#
exec_locals = {}
exec_(compiled, exec_globals, exec_locals)


## provider 深度优化总结

#### 1. 内存复用 (Memory Reuse)
*   **SQLAlchemy 路径**：通过 `__cinit__` 预分配 NumPy 数组。在 `async for` 循环中完全消除了 `list.append` 产生的 `realloc` 开销
*   **Arrow 路径**：利用 `batch.slice()`。Arrow 的切片只是指针的操作（Offset 和 Length 的改变），完全没有内存拷贝

#### 2. 属性访问优化 (Attribute Access)
*   **去字典化**：所有 `Index`/`Adjust` 类不再使用 `c_batch["date"]` 这种字符串哈希查找，而是直接访问 `self._buf_date`（C 结构体偏移量访问）
*   **去反射化**：移除了 `getattr(response, key)`，改为显式的属性调用

#### 3. 向量化填充 (Vectorized Proto-fill)
*   Protobuf 的 Python 扩展在处理 `extend(numpy_array)` 时，底层会尝试使用特定的优化路径（尤其是在启用 `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=cpp` 时），这比 Python 循环快一个数量级

#### 4. 类型安全与转换
*   在 `Tick` 和 `Close` 中，`to_numpy(zero_copy_only=True)` 是性能的“防火墙”。如果数据不是连续内存，它会报错提醒你，而不是默默地在后台进行昂贵的拷贝

数据提供层（Data Provider）在处理高频 Tick 或大规模历史数据时，CPU 使用率将主要集中在数据库 I/O 和序列化上，而 Python 虚拟机的调度开销将降至最低


`__cinit__` vs `__init__` 的本质区别

*   **`__cinit__` (C-level Constructor)**:
    *   **触发时机**：在对象的内存被分配后**立即**调用。
    *   **核心用途**：专门用于初始化 **纯 C 成员**（如 `malloc` 申请的 C 指针、C++ 的 `new` 对象）。
    *   **状态**：此时 Python 对象可能尚未完全准备好。虽然你可以调用 `np.empty`，但如果初始化过程中抛出异常，由于 `__cinit__` 保证会被执行，可能会导致复杂的垃圾回收问题。
*   **`__init__` (Python-level Constructor)**:
    *   **触发时机**：在 `__cinit__` 完成之后调用。
    *   **核心用途**：初始化 **Python 对象成员**（如 `list`、`dict`、`np.ndarray`）。
    *   **状态**：此时对象已经是一个完整的 Python 扩展对象。

# joblib replace mp avoid pickle instead of dill or cloudpickle

to_numpy(zero_copy_only=False) not suitable for string

# # grp by sid indices
# c_indices = np.where(_buf_sid[:num_rows-1] != _buf_sid[1:])[0] + 1 
# s_indices = np.insert(c_indices, 0, 0)
# e_indices = np.append(c_indices, num_rows)

git rm --cached -r rpc_feed/core/gateway/duckdb/cache/duckdb_macro.db # undo track

# window install  msys2
msys2 
pacman -Syu # update database
pacman -S rsync openssh

UCRT：全称 Universal C Runtime（通用 C 运行时），是 Windows 10/11 内置的底层 C 库（替代老旧的 msvcrt.dll），微软官方维护，适配最新系统特性；
64：64 位架构（对应 Windows 64 位系统）；

# delete means dst == src on msys2 ucrt64
# export MSYSTEM=UCRT64  # 指定环境为 UCRT64
# exec bash               # 重启 shell 生效
rsync -avzP --delete src dst

# smb sever message block \\电脑IP\共享文件夹名
# msys2 cd /c/
# rsync -avzP minute/ hengxinliu@192.168.64.1:/Users/hengxinliu/Downloads/rsync/

# 临时挂载（当前会话有效）
mkdir -p /c
mount C:/ /c

# 改为继承 Windows PATH 并挂载盘符
MSYS2_PATH_TYPE=inherit

# 或者修改 /etc/fstab 永久生效
echo 'C:/ /c ntfs noacl,auto' >> /etc/fstab

. **绝对不要用读写替换（Read-Modify-Write）！** 5000 次 I/O 的写放大不仅拖慢行情处理，而且一旦爬虫中途断电，这 5000 个被覆盖到一半的 Parquet 文件将全部损坏，导致灾难性的回撤。
2. **最快落地方案：修改 `basename_template`（采纳方案一）。**
   保留您现在的代码架构，只需要改 2 行代码。把每天新爬取的分钟数据，作为一个名字叫 `day_18.parquet` 的新文件直接写入现有目录。Polars 的强大之处在于它根本不在乎一个目录下有几个 Parquet 文件，`scan_parquet` 会在微秒级把它们当成一张表来处理。
3. **周末跑个批处理：** 写一个简单的 Python 脚本，用 crontab 挂在周六凌晨 2 点。把这个月碎掉的文件读进来，写出一个大的 `base.parquet`，然后删掉碎文件。这就构成了现代数据湖最经典的 **Lambda 架构**！


# # table transfer dataframe
# df = table.to_pandas()
# pa.Table.from_pandas(df, schema=table.schema)
# df_month = pl.scan_parquet("dataset/year_month=202603/*.parquet").collect()
# df_month = pl.scan_parquet("dataset/year_month=202603/*.parquet").collect()

# batch_results = await asyncio.wait_for( # block util finish 
#     loop.run_in_executor(None, self._process_batch, batch), # return Future
#     timeout=TICK_PROCESS_TIMEOUT)


# Parquet 格式最强大的两个特性：**分区裁剪（Partition Pruning）** 和 **谓词下推（Predicate Pushdown）**

您构建的 `year=YYYY/quarter=QQ/sid=XXXXXX` 这种目录结构叫做 **Hive Partitioning**，这是大厂数据湖的标配
Polars 的惰性引擎（Lazy API）极其聪明，只要您在 `scan_parquet` 之后接上 `.filter()`，它会在**读取磁盘之前**分析查询条件
1. **分区裁剪**：如果您查 `start_date=20260301`，它连 `year=2005` 这个文件夹看都不会看一眼（零 I/O 开销）
2. **谓词下推**：对于符合条件的文件夹，它只读取 Parquet 文件内部符合日期和 `sid` 的数据块（RowGroup）

.zfill(6)：字符串的补零方法，参数 6 表示最终要生成的字符串长度 < 6 在左侧补 0；≥ 6：直接返回原字符串，不做修改

lazy_df = pl.scan_parquet(pattern)
lazy_df = (
    lazy_df
    # .filter(pl.col("date") >= "2024-01-01") 
    # .select(["col1", "col2", "col3"])         
    # .with_columns(pl.col("price").cast(pl.Float64))
)
# # execution plan
# print(lazy_df.explain()) 
self._row_iter = self._make_iter()

# lazy_df.sink_parquet(
#         output_path,
#         compression="zstd",  # 高压缩比
#         row_group_size=100_000,
#         maintain_order=False  # 更快
# )

<!-- # force numpy run on one core
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"  -->

#### A. 零拷贝导致内存竞态（Segment Fault 的直接原因）
当你使用 `self._buf_first_trading = np.empty(...)` 初始化一个 NumPy 数组，并在 `_flush` 中使用 `pa.array(self._buf_first_trading[:count])` 时：
1. **PyArrow 的零拷贝**：对于 NumPy 数组，`pa.array` 会尽可能进行零拷贝（Zero-Copy），这意味着 Arrow 数组并没有复制数据，它的底层指针**直接指向了你预先分配的 `self._buf_first_trading` 这块内存**。
2. **异步复写**：你通过 `yield batch_to_resp(batch)` 将数据抛出（通常交给了 gRPC 或其他异步网络层进行序列化发送）。这需要一定时间。
3. **覆盖正在被使用的内存**：此时 `async for` 循环并没有停止，它进入了下一个循环，并执行 `self._buf_first_trading[i] = row[2]`。
4. **爆炸**：底层的 C++ Arrow IPC 序列化器正在读取这块内存，而 Cython 此时正在往这块内存里写新的值。C++ 读取了被破坏的数据结构，甚至遇到了错位的内存对齐，直接导致 Segmentation Fault。

在 Cython 中，`.pxd` 文件的角色等同于 C/C++ 中的 **头文件（Header files, `.h`）**。
它的作用是**“声明（Declaration）”**，而不是**“定义和初始化（Definition & Initialization）”**。

当你把 `cdef long CHUNK_SIZE = 1024` 写在 `.pxd` 中时：
1. Cython 编译器看到 `.pxd` 文件，把它翻译成了 C 语言的 `extern long CHUNK_SIZE;`（意思是在别的地方有一个叫这个名字的变量）。
2. **`.pxd` 文件不会生成可执行的初始化代码。** 也就是说，赋值操作 `= 1024` 被编译器直接忽略或者丢弃了。
3. 在底层的 C 语言标准中，**未显式初始化的全局变量，默认值会被填充为 `0`。**


这个错误的根本原因在于 Cython 的语法机制。你在头部执行了 `from libc.time cimport tm`，此时 `tm` 在 Cython 的上下文中已经是一个合法的**类型（Type）**，不需要（也不能）再像纯 C 语言那样在前面加上 `struct` 关键字。加上 `struct` 会让 Cython 解析器完全懵掉，从而报出 `Syntax error`

这是一个非常触及 Cython 核心底层设计的问题。

简单来说：**因为 `pxd` 中定义的 `struct` 是纯粹的 C 语言底层数据结构，而 Python 的 `import` 只能识别和引入 Python 对象（即继承自 `PyObject` 的东西）

### 1. 编译期 vs 运行期
*   **`.pxd` 文件相当于 C/C++ 中的头文件 (`.h`)**。它只是纯粹的**声明**，给 Cython 编译器看的。它本身在编译后不会生成任何可以直接被 Python 虚拟机执行的独立模块（`.so` 或 `.pyd`）。
*   **`cimport` 发生在编译期**。当你在 `.pyx` 中 `cimport` 一个结构体时，Cython 编译器在生成 C 代码时，会直接把对应的 C `struct` 定义引入进来。
*   **`import` 发生在运行期**。Python 在运行时去硬盘上找编译好的动态链接库（`.so`/`.pyd`）或 `.py` 源码。纯 C 的 `struct` 在编译成二进制后，只是一段连续的内存布局，没有 Python 类应该有的属性字典（`__dict__`）、方法表、引用计数（`ob_refcnt`）等。Python 解释器根本不知道怎么解析这块纯 C 内存。

### 2. C 空间 (C-Space) 与 Python 空间 (Python-Space) 隔离
在 Cython 中，世界被严格分为两半：
*   **C 空间**：`cdef int`，`cdef struct`，`cdef void func()`。这些追求极致性能，没有任何 Python 额外开销，但 Python 无法直接接触它们。
*   **Python 空间**：`def func()`，`class`，`list`，`dict`。这些是 Python 对象，带引用计数，性能有损耗，但能被外部 `import`。

`struct` 属于绝对的 C 空间，因此只能在 Cython 内部通过 `cimport` 互相调用

# airflow 重构 rpc_feed dag 功能
poetry add apache-airflow
airflow db migrate # initialize db
airflow scheduler # dags / plugins

find bt_core -type f \( -name "*.so" -o -name "*.cpp" \)  -print0 | xargs -0 rm -f

# airflow config  priority > cfg
export AIRFLOW_HOME=$(pwd)/airflow_home
export PYTHONPATH=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/bt_studio/pipeline/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# consistent
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://quant_user:quant_password@127.0.0.1:5432/quant_db
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

rm -f $AIRFLOW_HOME/airflow.db

poetry run airflow db migrate

poetry run airflow dags list

poetry run airflow dags test fsm_wfo_pipeline_v3

airflow db reset -y

airflow dags delete example_bash_operator -y
airflow dags delete example_branch_operator -y

airflow dags report-import-errors

poetry run airflow dags list-import-errors

export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=9000
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Shanghai

poetry run airflow scheduler -D
poetry run airflow webserver -p 8080 -D
