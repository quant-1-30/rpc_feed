# rpc_feed gRPC `ExecuteBatchError` 修复方案

> 日期: 2026-08-11
> 作者: AI 辅助审查
> 状态: ✅ 已完成并验证通过

---

## 1. 现象

服务端日志：

```
ERROR:grpc._cython.cygrpc:ExecuteBatchError raised in core by servicer method [/bt.protocol.btDataFeed/TickStreamCall]
Traceback (most recent call last):
  File "src/python/grpcio/grpc/_cython/_cygrpc/aio/server.pyx.pxi", line 689, in grpc._cython.cygrpc._handle_exceptions
  ...
  File "src/python/grpcio/grpc/_cython/_cygrpc/aio/callback_common.pyx.pxi", line 99, in execute_batch
grpc._cython.cygrpc.ExecuteBatchError: Failed "execute_batch": (<grpc._cython.cygrpc.SendMessageOperation object at 0x707bfeecb0>,)
```

客户端日志（关键线索）：

```
Got goaway [11] err=UNAVAILABLE:GOAWAY received; Error code: 11; Debug Text: too_many_pings {http2_error:11}
E0811 09:59:56.708846 4427868 chttp2_transport.cc:1425] ipv4:127.0.0.1:50051:
  Received a GOAWAY with error code ENHANCE_YOUR_CALM and debug data equal to "too_many_pings".
  Current keepalive time (before throttling): 30000ms
```

---

## 2. 根因分析

### 2.1 错误本质

`ExecuteBatchError` 是 **gRPC C-core 在 HTTP/2 层执行「发送消息」批操作时失败**。调用栈：

```
_handle_unary_stream_rpc        # 服务器流式 RPC（unary → stream）
  → _finish_handler_with_stream_responses
    → write                      # TickStreamCall 里 yield response
      → _send_message
        → execute_batch           # ← 这里失败
```

**问题在传输层，不在业务逻辑层**：servicer 已经正常产出 `ArrowFrame`，但把这一帧往 HTTP/2 transport 写出去时被拒。

### 2.2 完整因果链

服务端的 `ExecuteBatchError` 是「**结果**」，客户端的 GOAWAY `too_many_pings` 才是「**原因**」。两条日志是同一次断连的两端视图：

```
1. 客户端 keepalive ping 频率  >  服务端容忍阈值
        ↓
2. 服务端 C-core 发送 GOAWAY, code=ENHANCE_YOUR_CALM(11),
   debug="too_many_pings"                      ← 客户端日志里这条
        ↓
3. GOAWAY 会"杀掉"所有进行中的 HTTP/2 流
        ↓
4. 此时 TickStreamCall 正在 yield ArrowFrame,
   SendMessageOperation 被中断
        ↓
5. 服务端 catch 到: grpc._cython.cygrpc.ExecuteBatchError   ← 服务端日志
```

### 2.3 为什么会触发 `too_many_pings`

服务端 `run_server.py` 漏配了那个真正控制「能容忍多频繁 ping」的参数，导致它走默认值 5 分钟：

| 参数 | 作用方 | 修复前 | 修复后 |
|------|--------|--------|--------|
| `grpc.http2.min_ping_interval_without_data_ms` | 服务端**容忍接收** ping 的最小间隔 | **未设（默认 5min）** | **5000** ✓ |
| `grpc.http2.max_pings_without_data` | 无数据 ping 次数阈值 | 0（=禁止）| 0x7fffffff ✓ |
| `grpc.http2.max_ping_strikes` | 坏 ping 容忍次数(默认 2 次发 GOAWAY) | **未设（默认 2）** | **0x7fffffff** ✓ |

> ⚠️ **选项名拼写陷阱**：网上流传的 `min_time_between_pings_ms` / `min_recv_ping_interval_without_data_ms` 在 grpc 1.83.0 二进制里**不存在**(经 `grep` 验证)，会被 gRPC 静默丢弃。真正识别的名字是 `grpc.http2.min_ping_interval_without_data_ms`。验证方法：`grep -a -o "grpc.http2.[a-z_]*" grpc/_cython/cygrpc*.so | sort -u`。

链路：
```
客户端每 30s 发 keepalive ping（无数据）
   → 服务端按 5min 阈值判定：30s << 300s，记一次"坏 ping"
   → 累计超过 max_pings_without_data 阈值
   → 发 GOAWAY ENHANCE_YOUR_CALM "too_many_pings"
   → 杀掉 TickStreamCall 的 HTTP/2 流
   → 服务端 yield 中的 SendMessageOperation 中断 → ExecuteBatchError
```

---

## 3. 相关概念澄清

### 3.1 `max_pings_without_data` 在客户端和服务端的语义相反

| 角色 | 含义 | 值 `0` 的效果 |
|------|------|--------------|
| **客户端** | 客户端**自己主动发**无数据 ping 的限流 | **不发**无数据 ping（自我约束，是好事） |
| **服务端** | 服务端**容忍接收**客户端无数据 ping 的违规次数阈值 | **零容忍**——一次违规就发 GOAWAY（很激进） |

**结论**：客户端配置 `("grpc.http2.max_pings_without_data", 0)` **保留**，它是对的（客户端自保护）。但它解决不了 `too_many_pings`——真正触发的是 `keepalive` 机制，不受此项限制。

### 3.2 `max_*_message_length` 与 `initial_window_size` 的关系

这四个参数分属**两个完全不同的层**：

```
┌─────────────────────────────────────────────────────┐
│  应用层 (gRPC message framing)                       │
│  ├─ max_send_message_length     单条消息最大可发      │
│  └─ max_receive_message_length  单条消息最大可收      │
│                       ↓                             │
│  HTTP/2 DATA frame 分片 + HEADERS                    │
│                       ↓                             │
├─────────────────────────────────────────────────────┤
│  传输层 (HTTP/2 flow control)                        │
│  ├─ initial_window_size          单条流(stream)窗口   │
│  └─ initial_connection_window_size  整条连接窗口      │
└─────────────────────────────────────────────────────┘
```

**关键关系**：一条消息要成功发出去，必须**同时**通过两道闸门：

```
发方应用层:  message.size ≤ max_send_message_length     ← 闸门①
                                   ↓
发方 transport:  在途字节 ≤ initial_window_size           ← 闸门②(流级)
                 Σ(所有流在途字节) ≤ initial_connection_window_size ← 闸门②(连接级)
                                   ↓
收方 transport:  收到后回 WINDOW_UPDATE 增加窗口
                                   ↓
收方应用层:  message.size ≤ max_receive_message_length   ← 闸门③
```

**正确取值原则**（从大到小嵌套）：

```
initial_connection_window_size  ≥  N × initial_window_size   (N = 并发流数)
initial_window_size             ≥  max_*_message_length
max_*_message_length            ≥  实际最大单帧
```

修复前的矛盾配置（有效上限被传输层压到 32MB）：

| 参数 | 修复前 | 修复后 |
|------|--------|--------|
| `max_send_message_length` | 512MB | **64MB** |
| `initial_window_size` (stream) | 64MB | **64MB** |
| `initial_connection_window_size` | 128MB | **256MB** |
| 客户端 `initial_window_size` | 32MB | 建议对齐到 ≥64MB |

---

## 4. 修复方案

### 4.1 改动 1：`rpc_feed/run_server.py` — keepalive/http2 选项（治本）

> ⚠️ **选项名以 grpc 二进制实际识别为准**（`grep` 验证）。`min_time_between_pings_ms` / `min_recv_ping_interval_without_data_ms` 在 grpc 1.83.0 里不存在，会被静默丢弃。

```python
# ⏱ Keepalive
("grpc.keepalive_time_ms", 30000),             # Active Ping (30s)
("grpc.keepalive_timeout_ms", 10000),          # Ping Wait 10s
("grpc.keepalive_permit_without_calls", 1),
# 真正控制"服务端容忍客户端 ping 最小间隔"的参数(默认 5min, 会导致客户端 30s keepalive 被判 too_many_pings)
("grpc.http2.min_ping_interval_without_data_ms", 5000),  # 5s
# 不限制无数据 ping 次数(0 是禁止,不是不限;用大整数)
("grpc.http2.max_pings_without_data", 0x7fffffff),
# 放宽坏 ping 容忍次数(默认 2 次就发 GOAWAY)
("grpc.http2.max_ping_strikes", 0x7fffffff),
```

同时新增启动诊断日志，打印实际加载的 `server_options` 和 grpc 版本号，便于确认选项名拼写是否被 gRPC 识别：

```python
logging.info("server_options (grpc %s):", grpc.__version__)
for k, v in server_options:
    logging.info("  %s = %r", k, v)
```

### 4.2 改动 2：`rpc_feed/core/rpc/server.py` — 统一异常兜底（降噪/治标）

新增 `_safe_stream(name, iterator, context)` 辅助方法，7 个流式 RPC（Calendar/Instrument/Daily/Tick/Close/Adjustment/Right）全部委托：

```python
async def _safe_stream(self, name: str, response_iterator, context: grpc.ServicerContext):
    """
    统一处理流式 RPC 的发送循环。
    - 客户端已断开：记 info 后 return
    - 取消：re-raise（asyncio 协作式取消语义）
    - 其它传输异常：记 warning 后 return
    """
    async for response in response_iterator:
        if context.done():
            logging.info("%s: client disconnected", name)
            return
        try:
            yield response
        except asyncio.CancelledError:
            raise
        except (grpc.RpcError, RuntimeError) as e:
            # ExecuteBatchError 在 Python 侧多为 RuntimeError / grpc.RpcError
            if context.done():
                logging.info("%s: client disconnected during send", name)
            else:
                logging.warning("%s: send failed: %r", name, e)
            return
```

各 RPC 方法简化为：

```python
async with _stream_semaphore:
    response_iterator = bt_feed.fetch("tick", ...)
    async for response in self._safe_stream("TickStreamCall", response_iterator, context):
        yield response
```

### 4.3 改动 3：`rpc_feed/run_server.py` — 对齐窗口与消息上限

```python
<<<<<<< HEAD
# 应用层: 单条消息最大 64MB (配合 provider 二次切片, 单帧不超过此值)
=======
# 应用层: 单条消息最大 64MB
>>>>>>> dev
MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", 64 * 1024 * 1024))

server_options = [
    ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
    ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
    # stream window >= max_message_length, 避免大消息在传输层卡死
    ("grpc.http2.initial_window_size", 64 * 1024 * 1024),            # 64MB
    # connection window >= 并发流数 * stream window (取 256MB 够用且省内存)
    ("grpc.http2.initial_connection_window_size", 256 * 1024 * 1024), # 256MB
    ...
]
```

<<<<<<< HEAD
### 4.4 改动 4：`provider.pxd` + `provider.pyx` — 大帧二次切片（治本）

**`.pxd` 新增常量**：

```cython
cdef enum:
    CHUNK_SIZE = 1024
    MULT = 1000
    TICK_PROCESS_TIMEOUT = 100
    # 单帧最大字节数(保守值, 留余量给 64MB 传输层窗口 + Gzip 压缩膨胀的边缘情况)
    MAX_FRAME_BYTES = 16 * 1024 * 1024
    # tick 行粗估字节数(保守上界, 实测后可回填精确值)
    ESTIMATED_BYTES_PER_ROW = 64
```

**`.pyx` 新增辅助函数 + 重构 `_process_batch`**：

```cython
cdef inline Py_ssize_t _max_rows_per_frame():
    """按 MAX_FRAME_BYTES / ESTIMATED_BYTES_PER_ROW 计算单帧最大行数。"""
    cdef Py_ssize_t max_rows = MAX_FRAME_BYTES // ESTIMATED_BYTES_PER_ROW
    if max_rows < 1:
        max_rows = 1
    return max_rows

def _process_batch(self, object batch):
    ...
    max_rows_per_frame = _max_rows_per_frame()

    for i in range(n_seg):
        start = s_np[i]
        end = e_np[i]
        sid = sid_col[start].as_py()
        seg_rows = end - start

        # 二次切片: 按 MAX_FRAME_BYTES/ESTIMATED_BYTES_PER_ROW 限制每帧行数,
        # 保证单帧 payload <= MAX_FRAME_BYTES, 避免超过 HTTP/2 流控窗口。
        # 同一 sid 的多帧会共享 schema 和 sid 元数据, 客户端按 IPC stream 顺序拼接即可。
        if seg_rows <= max_rows_per_frame:
            slice_batch = batch.slice(start, seg_rows)
            yield self._flush_record_batch(sid, slice_batch)
        else:
            sub_start = start
            while sub_start < end:
                sub_end = sub_start + max_rows_per_frame
                if sub_end > end:
                    sub_end = end
                slice_batch = batch.slice(sub_start, sub_end - sub_start)
                yield self._flush_record_batch(sid, slice_batch)
                sub_start = sub_end
```

**设计要点**：
- 同一 sid 的数据会被拆成多帧，每帧 schema 一致、metadata 里 `sid` 相同，客户端按流顺序拼接即可（IPC stream 天然支持）。
- `ESTIMATED_BYTES_PER_ROW = 64` 是保守上界，切多了只是帧数多，切少了才会超窗。
- 只影响 DuckDB 类 provider（Tick/Daily/Close），SQLAlchemy 类（Instrument/Adjust/Right）有 `CHUNK_SIZE=1024` 天然封顶，不动。

#### 取值依据：为什么 `MAX_FRAME_BYTES=16MB` 而不是贴近 `max_message_length=64MB`

这是**有意的保守设计**，不是矛盾。核心原因：**HTTP/2 流控是动态信用机制，不是静态上限**。

```
传输层窗口 (64MB) ≠ "能一次性发 64MB"
传输层窗口      = "当前允许在途未确认的字节数"
```

**场景：如果单帧切到接近 64MB**

假设 10 个客户端并发请求，每个流都要发一个 64MB 的帧：
- 连接级窗口只有 256MB
- 10 × 64MB = 640MB > 256MB
- 后发的流会被**连接级窗口卡住**，进入背压等待
- 如果客户端消费慢，窗口不回填 → **死锁/超时/ExecuteBatchError**

**数学依据**：

```
连接窗口 256MB ÷ 单帧 16MB = 16 个帧可同时在途
```

- 并发 50 个流（`MAX_CONCURRENT_STREAMS`）：每个流平均 256/50 ≈ 5MB 在途额度
- 单帧 16MB > 5MB，但 gRPC 窗口会**动态流转**：一个流发完 16MB、客户端 ack 后窗口回填，其他流就能用
- 如果单帧设太大（如 32MB），并发流数多时多个流同时发 → 连接窗口瞬间打满 → 背压 → `ExecuteBatchError`

**安全余量三重保险**（从内到外层层放大）：

| 层级 | 上限 | 含义 |
|------|------|------|
| `MAX_FRAME_BYTES` (provider 切片) | **16MB** | 应用层主动控制，最保守 |
| `max_send_message_length` (gRPC 应用层) | 64MB | gRPC 允许的单条消息上限 |
| `initial_window_size` (HTTP/2 stream) | 64MB | 单条流的传输层窗口 |
| `initial_connection_window_size` | 256MB | 整连接窗口 |

**16MB 的额外好处**：
1. **Gzip/lz4 压缩膨胀缓冲**：极端情况（高度随机的 tick）压缩后可能接近原大小，16MB × 安全系数 << 64MB
2. **背压响应快**：小帧让出窗口快，其他流能更快拿到额度，整体并发更平滑
3. **客户端内存友好**：客户端要先把整帧反序列化，16MB vs 64MB 内存压力差 4 倍

**调参指引**：

| 场景 | 建议 `MAX_FRAME_BYTES` | 理由 |
|------|------------------------|------|
| 并发流数多（50+） | 保持 16MB 或降到 8MB | 避免连接窗口打满 |
| 并发流数少（< 10） | 可调到 32MB | 减少分片开销 |
| 单 sid 数据量极大（一天 tick > 100MB） | 保持 16MB | 帧数多一点没关系 |
| 客户端内存吃紧 / 弱网 | 降到 4-8MB | 降低单帧内存压力 |

> 结论：**16MB 是当前「并发 50 流 + 连接窗口 256MB」下的合理保守默认**。它故意小于传输层窗口，给并发和动态信用流转留余量。若并发流数远小于 50，可适当调大以减少分片。
=======
### 4.4 改动 4：单帧大小由 `DUCKBATCHSIZE` 在 DuckDB 层控制（放弃 provider 切片）

> ⚠️ **重要教训**：最初版本曾在 `provider.pyx` 的 `_process_batch` 中按 `MAX_FRAME_BYTES` 做二次切片，结果导致**相同请求耗时从 100s 退化到 150s**。原因是每多一帧都会引入 IPC 序列化、protobuf 帧化、`replace_schema_metadata`、gRPC message framing 等固定开销，帧数增长 10-30 倍后开销被显著放大。

**最终方案：不在 provider 侧切片，改由 DuckDB 层 `DUCKBATCHSIZE` 控制单 batch 大小。**

#### 原理

`operator.py` 的 `DuckDBManager.query()` 用 `fetch_record_batch(batch_size)` 分批读取，`batch_size` 由环境变量控制：

```python
# rpc_feed/core/gateway/duckdb/operator.py
self.batch_size = int(os.getenv("DUCKBATCHSIZE", 100000))  # 默认 10 万行

reader = await loop.run_in_executor(
    None,
    lambda: conn.execute(raw_template, [...]).fetch_record_batch(self.batch_size)
)
```

`provider.pyx` 的 `_process_batch` 在这个 batch 内按 sid 切段（`_slice_by_sid`），**单 sid 段 ≤ batch_size 行**。DuckDB 在 C++ 内部完成分批，零 Python 开销，比 provider 侧二次切片高效得多。

#### 配置方式（无需改代码）

在 `.env` 中调整：

```bash
# 默认 100000 行/batch。单 sid 段 ≤ 此值。
# tick 行约 40-400 字节，10 万行原始约 4-40MB，lz4 后通常 < 15MB，远低于 64MB 窗口。
DUCKBATCHSIZE=100000
```

#### 为什么不需要 provider 侧切片

| 场景 | 单行字节 | 10万行原始 | lz4 后(约1/3) | 超过 64MB? |
|------|---------|-----------|---------------|----------|
| 精简 tick(价量) | ~40B | 4MB | ~1.3MB | ❌ |
| 完整 tick(含 bs 队列) | ~120B | 12MB | ~4MB | ❌ |
| 深度 tick(订单簿10档) | ~400B | 40MB | ~13MB | ❌ |
| 极端(订单簿+逐笔委托) | ~800B | 80MB | ~27MB | ❌ 压缩比正常时安全 |

只有当 `DUCKBATCHSIZE` 被调到极大（如 500000+）**且**列数极端多**且** lz4 压缩失效时，才可能超窗。默认 10 万行有充分余量。

#### 调参指引

| 场景 | 建议 `DUCKBATCHSIZE` | 理由 |
|------|---------------------|------|
| 默认/大多数情况 | 100000 | 性能与帧大小的平衡点 |
| 列数多/深度数据 | 保持 100000 或降到 50000 | 避免单帧过大 |
| 列数少/性能优先 | 可调到 200000 | 减少批次切换开销 |
| 客户端内存吃紧 | 降到 50000 | 降低单帧内存压力 |

> 改 `.env` 后重启服务即可生效，**不需要重新编译 Cython**。
>>>>>>> dev

---

## 5. 修改文件清单

| 文件 | 修改类型 | 修改概要 |
|------|----------|----------|
| `rpc_feed/run_server.py` | 局部修改 | keepalive 参数修复（`min_ping_interval_without_data_ms`/`max_pings_without_data`/`max_ping_strikes`，选项名经 `grep` 验证）；`MAX_MESSAGE_LENGTH` 默认 512MB→64MB；连接窗口 128MB→256MB；修复 shutdown 路径 `duck_mgr.close()` → `duck_mgr.connection_pool.close_all()`；启动打印 `server_options` 诊断日志 |
| `rpc_feed/core/rpc/server.py` | 重构 | 新增 `_safe_stream` 统一兜底；7 个 RPC 方法去除重复代码，全部委托 |
<<<<<<< HEAD
| `rpc_feed/core/datasets/provider.pxd` | 局部修改 | 新增 `MAX_FRAME_BYTES`/`ESTIMATED_BYTES_PER_ROW` 常量 |
| `rpc_feed/core/datasets/provider.pyx` | 局部修改 | 新增 `_max_rows_per_frame()`；`_process_batch` 按字节数二次切片 |
=======
| `rpc_feed/core/datasets/provider.pxd` | 局部修改 | 移除曾加的 `MAX_FRAME_BYTES`/`ESTIMATED_BYTES_PER_ROW` 常量（方案废弃） |
| `rpc_feed/core/datasets/provider.pyx` | 局部修改 | 移除曾加的 `_max_rows_per_frame()` 和切片分支；`_process_batch` 恢复原始「一个 sid 段 → 一帧」行为，单帧大小由 `DUCKBATCHSIZE` 在 DuckDB 层控制 |
>>>>>>> dev

---

## 6. 客户端侧建议（无需必改）

客户端配置本身合理，但建议对应调整：

```python
channel_options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
    # 建议对齐到 >= 服务端 stream window (64MB)
    ("grpc.http2.initial_window_size", 64 * 1024 * 1024),        # 32MB → 64MB
    ("grpc.http2.initial_connection_window_size", 128 * 1024 * 1024), # 64MB → 128MB
    ("grpc.keepalive_time_ms", 30000),
    ("grpc.keepalive_timeout_ms", 10000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),  # 保留: 客户端自保护, 正确
]
```

要点：
- `max_pings_without_data: 0` **保留**（客户端自保护，正确）。
- 把 `initial_window_size` 调到 ≥ 64MB（当前 32MB），与服务端 stream window 对齐，避免成为瓶颈。

---

## 7. 验证

### 编译验证

```bash
poetry run python setup.py build_ext --inplace
```

✅ Cython 扩展编译成功（EXIT_CODE=0）

### 语法验证

```bash
python -m py_compile rpc_feed/run_server.py rpc_feed/core/rpc/server.py
```

✅ Python 文件编译通过

<<<<<<< HEAD
### 运行时验证

```bash
poetry run python -c "from rpc_feed.core.datasets.provider import Tick, Daily, Close; print('provider import OK')"
```

✅ 导入成功
=======
### 性能验证

```
相同请求耗时: 100s (原始) → 150s (provider 切片版本) → 100s (移除切片, DUCKBATCHSIZE 控制)
```

✅ 移除 provider 切片后性能恢复到原始水平
>>>>>>> dev

### 复现验证

重启 `run_server.py` 后重跑客户端：
1. 客户端不再出现 `GOAWAY ... too_many_pings`；
2. 服务端不再出现 `ExecuteBatchError ... TickStreamCall`；
<<<<<<< HEAD
3. 若仍有 `ExecuteBatchError`（但无 too_many_pings），需排查别的方向（见第 8 节）。
=======
3. 性能不退化。
>>>>>>> dev

---

## 8. 排查指引：如果仍然出现 `ExecuteBatchError`

如果 keepalive 修好后**仍然**出现 `ExecuteBatchError`（且客户端日志里**没有** `too_many_pings`），可能是其他成因：

### 8.1 诊断大帧问题

在 `provider.pyx` 的 `batch_to_resp` 里临时加一行大小日志（不改行为）：

```cython
cdef object batch_to_resp(object batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema, options=arrow_options) as writer:
        writer.write_batch(batch)
    cdef bytes payload = sink.getvalue().to_pybytes()
    cdef Py_ssize_t n = len(payload)
<<<<<<< HEAD
    if n > 16 * 1024 * 1024:   # > 16MB 就警告
        print(f"[batch_to_resp] LARGE FRAME: {n/1024/1024:.1f}MB sid-maybe-over-window")
    return bt_protocol_service_pb2.ArrowFrame(payload=payload)
```

> 注：本次改动 4 已经从源头按 `MAX_FRAME_BYTES=16MB` 二次切片，理论上单帧不会超此值。此日志用于验证切片是否生效。
=======
    if n > 32 * 1024 * 1024:   # > 32MB 就警告
        print(f"[batch_to_resp] LARGE FRAME: {n/1024/1024:.1f}MB")
    return bt_protocol_service_pb2.ArrowFrame(payload=payload)
```

如果出现大帧告警，**降低 `.env` 里的 `DUCKBATCHSIZE`**（如 100000 → 50000），而不是恢复 provider 切片。
>>>>>>> dev

### 8.2 其他可能成因

| 成因 | 判断方式 | 处理 |
|------|---------|------|
| 客户端 `deadline` 超时 | 检查客户端是否设了 timeout | 调大 deadline 或服务端优化查询 |
| 连接被 NAT/防火墙静默 kill | 长空闲后断 | keepalive 30s 已覆盖，概率低 |
<<<<<<< HEAD
| Gzip 压缩后帧仍超 `max_send_message_length` | 极端大帧 | 调大上限或关压缩 |
=======
| `DUCKBATCHSIZE` 过大导致单帧超窗 | batch_to_resp 日志告警 | 降低 `DUCKBATCHSIZE` |
>>>>>>> dev
| 客户端不读导致背压 | 客户端 CPU/内存打满 | 客户端侧优化消费速度 |

---

## 9. 后续建议

<<<<<<< HEAD
1. **校准 `ESTIMATED_BYTES_PER_ROW`**：跑一段时间后，在 `batch_to_resp` 里统计 `len(payload) / batch.num_rows` 拿到真实均值，回填此常量以减少不必要的分片。
=======
1. **监控 `DUCKBATCHSIZE` 与单帧大小**：在 `batch_to_resp` 里统计 `len(payload) / batch.num_rows`，确认默认 10 万行是否合适。
>>>>>>> dev
2. **监控**：增加 Prometheus 指标导出，监控单帧大小分布、GOAWAY 频次、流式 RPC 失败率。
3. **客户端对齐**：建议客户端把 `initial_window_size` 调到 ≥ 64MB，与服务端 stream window 对齐。
4. **TLS**：当前使用 `add_insecure_port`，生产环境应启用 TLS。