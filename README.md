# cython 重构 providers
# cython 重构 model and  util
# cython 重构 rpc server

    # register_date:登记日 ; ex_date:除权除息日 
    # 股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
    # 上交所证券的红股上市日为股权除权日的下一个交易日; 深交所证券的红股上市日为股权登记日后的第3个交易日
    # bonus_share --- 送股 / transfer --- 转股 / bonus --- 股息

    # register_date:登记日 ; ex_date:除权除息日; pay_date:除权除息日 ; effective_date:上市日期 
    # 股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
    # 上交所证券的红股上市日为股权除权日的下一个交易日; 深交所证券的红股上市日为股权登记日后的第3个交易日
    # price --- 配股价格 / ratio --- 配股比例

# blob to transform string to bytes
# alembic init alembic --template gener / async 
# alembic revision -m "change sid and name from str to bytes"

# python -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. service.proto 

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

# rsync -avzP minute/ hengxinliu@192.168.64.1:/Users/hengxinliu/Downloads/rsync/

. **绝对不要用读写替换（Read-Modify-Write）！** 5000 次 I/O 的写放大不仅拖慢行情处理，而且一旦爬虫中途断电，这 5000 个被覆盖到一半的 Parquet 文件将全部损坏，导致灾难性的回撤。
2. **最快落地方案：修改 `basename_template`（采纳方案一）。**
   保留您现在的代码架构，只需要改 2 行代码。把每天新爬取的分钟数据，作为一个名字叫 `day_18.parquet` 的新文件直接写入现有目录。Polars 的强大之处在于它根本不在乎一个目录下有几个 Parquet 文件，`scan_parquet` 会在微秒级把它们当成一张表来处理。
3. **周末跑个批处理：** 写一个简单的 Python 脚本，用 crontab 挂在周六凌晨 2 点。把这个月碎掉的文件读进来，写出一个大的 `base.parquet`，然后删掉碎文件。这就构成了现代数据湖最经典的 **Lambda 架构**！


# # table transfer dataframe
# df = table.to_pandas()
# pa.Table.from_pandas(df, schema=table.schema)
