# rpc_feed 数据湖 tick/datetime 一致性审计与修复

> 日期: 2026-10-16
> 作者: AI 辅助审查
> 状态: ✅ 已完成并全量验证通过(全湖 1,055,268 个 parquet 文件 100% 一致-UTC正确)
> 关联文档: `optimize_plan.md`(并发修复)、`pandas_blknos_bug_fix.md`(writer 加固)、`grpc_execute_batch_error_fix.md`(传输层修复)

---

## 1. 背景

`UniverseDateParser`(CSV 入库路径)被发现存在时区语义 bug:naive 墙钟直接 `astype("int64")` 取 epoch,
与 `StructDateParser`(.01 二进制路径)产出的 tick 相差 8 小时。由于**原始 raw CSV 已不在本地**,
无法通过重跑管线修复,需要:

1. 对现有数据湖(`~/Downloads/quant/parquet`,stock/fund/benchmark 三类型共 1,055,268 个文件)
   做**只读体检**,判定哪些文件受影响、影响到什么程度;
2. 修正 CSV 管线代码,保证**未来**入库正确;
3. 备份并**离线修复**受损文件,最终全量复验。

---

## 2. 审计方法(全程只读)

### 2.1 核心签名:delta

```sql
-- 连接必须 SET TimeZone='UTC'(原因见 2.3)
SELECT filename, count(*),
       min(tick - CAST(epoch(datetime) AS BIGINT)),   -- delta
       max(tick - CAST(epoch(datetime) AS BIGINT)),
       min(extract(hour from datetime)), max(extract(hour from datetime))
FROM read_parquet('<glob 或文件列表>', filename=true)
GROUP BY filename
```

| delta | datetime UTC 小时 | 分类 | 含义 |
|-------|------------------|------|------|
| 0 | 1~7 | ✅ 一致-UTC正确 | tick == epoch(datetime),绝对时间为真 UTC |
| 0 | 9~15 | ⚠️ 墙钟存储 | 两列互相一致,但都是北京墙钟当 UTC,整体 +8h |
| +28800 | — | ❌ 不一致 | tick 比 datetime 晚 8h(本例:tick 对、datetime 早 8h) |
| -28800 | — | ❌ 不一致 | tick 比 datetime 早 8h |

A 股分钟数据 UTC 小时落在 1~7(北京 09:30-15:00),可作绝对时间正确性的旁证。

### 2.2 schema 分代(footer 只读分类)

用 `pyarrow.ParquetFile(f).schema_arrow` 只读 footer 判定 `datetime` 列家族:

- **naive**(`timestamp[ms]`):旧 writer 产物,占湖内绝大多数(2024/2025 及以前全部);
- **tz**(`timestamp[ms, tz=UTC]`):现行 writer(`_make_schema` 声明 `tz="UTC"`)产物,
  **仅出现在 2026 年**分区。

两类文件服务端查询均正确(`ConnectionPool` 固定 `SET TimeZone='UTC'`,模板对 naive/tz 都按 UTC 比较)。

### 2.3 审计工具自身的两个时区陷阱(重要教训)

1. **TIMESTAMPTZ 的 `extract(hour)` 按会话时区渲染**:本机默认 Asia/Shanghai,
   正确的 UTC 瞬间(01:30 UTC)会渲染成 9~15 点,**被误判为墙钟文件**。
   审计连接必须 `SET TimeZone='UTC'`。
2. **混合 schema 批次的统一化假象**:一次 `read_parquet` 同时喂 naive 与 tz 文件时,
   DuckDB 统一 schema 会把 tz 列按会话时区降为 naive,产生 delta=±28800 的**假异常**。
   对策:先按 footer 分组,同质组内扫描;或全程序固定 UTC 会话。

另外:**这些 parquet 物理上只有 8 个数据列**(`amount/close/datetime/high/low/open/tick/volume`),
`year/quarter/sid/date` 是 DuckDB 从 hive 路径自动推导的——`DESCRIBE` 看到的 12 列中有 4 列来自路径。
用 pyarrow 读写时所见即物理列。

---

## 3. 审计结果(修复前)

覆盖:全湖 1,055,268 个文件**全量扫描**(benchmark 765 / fund 131,662 / stock 922,841)。

| 分类 | 文件数 | 占比 |
|------|--------|------|
| ✅ 一致-UTC正确 | 1,037,586 | 98.32% |
| ⚠️ 墙钟存储(整体+8h) | 17,669 | 1.67% |
| ❌ 不一致(tick 对、datetime 早 8h) | 13 | 0.001% |

- 墙钟文件全部集中在 `year=2007`(stock 17,609/18,654 = 94.4%;fund 60/356 = 16.9%);
  2006/2008 及其余所有年份全部正确;
- 13 个不一致文件全部是 2026 年 Q2/Q3 分区(清单见附录);
- daily/close topic **两类问题都不受影响**(日切分与 arg_min/arg_max 对常数平移不变);
  受影响的只有 tick topic 的时间窗过滤与 tick 取值。

---

## 4. 根因分析

### 4.1 2007 墙钟文件(17,669 个)

早期 writer 版本不做时区转换:datetime 保持北京墙钟 naive(09:31),
tick = 墙钟当 UTC 的 epoch。两列一致地整体 +8h。

### 4.2 2026 年的 13 个文件

逐行验证签名(tick=北京 09:31 的真 UTC epoch、datetime 早 8h)反推:
**该批原始 CSV 的时间列是 UTC 墙钟(01:31)**,且经旧 parser + 现行 writer 组合处理:

- 旧 parser(naive 当 UTC 取 epoch)恰好把 tick 算对——"误打误撞";
- writer 的 `_make_partition` 固定 `tz_localize("Asia/Shanghai")`,把 UTC 墙钟当北京墙钟,
  datetime 被多减 8 小时。

### 4.3 旧代码正确性矩阵("误打误撞"只救一半)

| 源 CSV 时区 | 旧 parser + 旧 writer(2007 时代) | 旧 parser + 现行 writer(2026) |
|------------|----------------------------------|------------------------------|
| 北京墙钟(09:31) | tick 晚 8h、datetime 墙钟(整体+8h) | tick 晚 8h、datetime 对 |
| UTC 墙钟(01:31) | (未出现) | **tick 对、datetime 早 8h** ← 13 个文件 |

结论:旧代码的正确性依赖"源时区恰好是 UTC"的运气;两种源墙钟 × 新旧代码只有修复后的
parser 能全对。

---

## 5. 代码修复(面向未来入库,已验证)

| 文件 | 修改 | 说明 |
|------|------|------|
| `core/graph/node/format.py` `UniverseDateParser` | 新增 `tz` 参数=**源 CSV 时间列时区**;tick 统一为真 UTC epoch;datetime 归一化为北京 naive 墙钟(与 writer 的上海时区契约对齐) | 北京墙钟源用默认;UTC 墙钟源(2026 那批供应商)在 graphml 配 `"tz": "UTC"` |
| `core/graph/node/format.py` `Multiply` | `.mul(...).round(0).astype(int64)` | 消除浮点伪影截断(12.34×1000=12339.999…→旧代码 12339),与 PG 侧 `round()` 对齐 |
| `core/graph/node/loader.py` `TextLoader` | rename 后按新列名再应用一次 dtype | 纯数字代码(399006)整列被推断为 int64 导致 `.str` 崩溃;此前靠混有 `1A0001` 侥幸为 str |

端到端验证:临时目录中以 `benchmark_csv.graphml` 同参数跑 `TextLoader→UniverseDateParser→Multiply→Dtypes→ParquetWriter`,
北京墙钟与 UTC 墙钟两种源产出**完全一致且正确**的 parquet(tz schema、delta=0、UTC 小时 1~7、
分区正确、浮点已 round),13/13 断言通过。

> 同期还完成了 RPC/服务端修复(run_server 双模块导入分裂、`schema_range` 畸形日期死循环、
> DuckDB reader 归还前 close、限流器 handler 形态等),详见 git diff 与 `optimize_plan.md` 续篇,
> 与本次数据修复无关,不在此展开。

---

## 6. 数据修复(本次执行)

### 6.1 备份

- 目标:`~/Downloads/quant/backup/`,保留 `类型/year=/quarter=/sid=/date=/` 相对路径;
- 数量:17,682 个文件(stock 2007 17,609 + fund 2007 60 + 2026 的 13),共 2,325.8 MB;
- 逐文件记录 `~/Downloads/quant/backup/fix_manifest.jsonl`(`fixed/already_ok/mismatch/error` 状态)。

### 6.2 修复规则

| 批次 | 文件数 | 变换 | SQL(核心) |
|------|--------|------|-----------|
| 2007 墙钟 | 17,669 | `tick-28800`,`datetime-8h` | `SELECT * REPLACE (CAST(tick-28800 AS BIGINT) AS tick, CAST(datetime - INTERVAL 8 HOUR AS TIMESTAMP) AS datetime) FROM read_parquet(f, hive_partitioning=false)` |
| 2026 异常 | 13 | `datetime+8h`,tick 不动 | `SELECT * REPLACE (CAST(datetime + INTERVAL 8 HOUR AS TIMESTAMPTZ) AS datetime) FROM read_parquet(f, hive_partitioning=false)` |

要点:

- **幂等**:每文件修复前重新校验签名(墙钟:`delta==0 且小时≥8`;2026:`delta==+28800`),
  不匹配则跳过并记录,中断重跑不会二次平移;
- **原子替换**:DuckDB 取 Arrow 表 → `cast` 回**原文件 schema**(保 ms 精度与 naive/tz 家族)
  → snappy 写同目录临时文件 → `os.replace`;
- **`hive_partitioning=false`**:`SELECT *` 只返回 8 个物理列,与 `pq.read_schema` 对齐
  (否则 hive 路径列混入导致 cast 失败——首轮 17,682 个全部安全失败未动数据,修正后通过);
- 分区列(`date=` 等)无需改:两批修复后行仍落在原月份(09:31-8h=01:31 同日;
  早 8h 的 datetime+8h 回到同月)。

### 6.3 执行结果

```
fixed=17682  already_ok=0  mismatch/error=0  (23s)
修复范围即时复验: 剩余 wall2007=0, 剩余 bad2026=0
```

---

## 7. 最终验证

### 7.1 逐行对比(备份 vs 修复后,18/18 断言通过)

| 检查项 | 结果 |
|--------|------|
| 行数一致 | ✅(4,800 vs 4,800) |
| OHLCV 六列逐值完全一致 | ✅(未动任何数值) |
| datetime/tick 变化精确符合预期 | ✅(2007:各 -8h/-28800;2026:datetime +8h、tick 不变) |
| schema 家族保持 | ✅(2007=timestamp[ms];2026=timestamp[ms, tz=UTC]) |
| 修复后 UTC 小时进入 1~7 | ✅ |

修复后行级示例:
- 2007:`datetime=2007-12-03 01:31 UTC, tick=1196645460, delta=0`(北京 09:31);
- 2026:`datetime=2026-07-01 01:31 UTC, tick=1782869460, delta=0`。

### 7.2 全湖复扫

```
文件总数: 1,055,268   (benchmark 765 + fund 131,662 + stock 922,841)
一致-UTC正确          1,055,268  (100.00%)
```

---

## 8. 遗留事项与运维建议

1. **备份保留**:`~/Downloads/quant/backup/`(2.3 GB)建议观察 1~2 个交易日
   (尤其 2007 区间 tick 查询)后再清理;
2. **服务无需为 parquet 重启**:DuckDB 每查询重读文件,新查询立即看到修复后数据;
   但第 5 节的代码修复需重启 `run_server.py` 生效;
3. **未来该供应商 CSV 重刷**:graphml 的 `UniverseDateParser` 节点配 `"tz": "UTC"`;
   源墙钟判据——开盘分钟显示 `09:31` 为北京墙钟,`01:31` 为 UTC;
4. **入库后巡检**:按 2.1 的 delta 签名对新分区抽检,异常(delta≠0 或小时 9~15)即阻断;
5. **湖内 schema 两代共存**(naive/tz)属已知状态,查询层兼容,无需统一;
   若未来统一,建议以 tz 版为准(现行 writer 行为)。

---

## 附录 A:2026 年 13 个异常文件清单(已修复)

```
stock/year=2026/quarter=Q3/sid=000950/date=202607/part-0.parquet
stock/year=2026/quarter=Q3/sid=002178/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=002658/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=300357/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=300801/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=301229/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=301263/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=600161/date=202607/part-0.parquet
stock/year=2026/quarter=Q3/sid=399994/date=202608/part-0.parquet
stock/year=2026/quarter=Q3/sid=603661/date=202607/part-0.parquet
stock/year=2026/quarter=Q3/sid=605162/date=202608/part-0.parquet
fund/year=2026/quarter=Q2/sid=159958/date=202606/part-0.parquet
fund/year=2026/quarter=Q3/sid=167302/date=202607/part-0.parquet
```

## 附录 B:2007 墙钟文件分布(已修复)

- `stock/year=2007`:17,609 / 18,654(94.4%,四个季度均有);
- `fund/year=2007`:60 / 356(16.9%);
- 同年其余文件为正常 UTC(混有多个入库批次的痕迹)。

## 附录 C:审计/修复关键 SQL 与脚本逻辑

- 全湖扫描:按 `类型/year/quarter` glob + `filename=true` 分组聚合 delta(全文扫描约 5 分钟);
- 单文件签名:`SELECT min/max(tick - CAST(epoch(datetime) AS BIGINT)), min(extract(hour from datetime))`;
- 修复:见 6.2;备份后幂等修复 + `os.replace` 原子替换 + manifest。
