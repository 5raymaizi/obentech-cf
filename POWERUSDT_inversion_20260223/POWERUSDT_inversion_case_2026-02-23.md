# POWERUSDT 倒挂案例分析备忘（北京时间 2026-02-23 23:00 之后）

## 1. 目标
这份文档用于固定本次“资金费率方向与跨所价差方向相反（极端倒挂）”事件的分析输入、字段口径和执行流程，后续可以直接复用到其他币种。

## 2. 已确认数据位置
- 深度数据目录：`/Volumes/T7/Obentech/data`
- Funding 数据目录：
  - Binance：`/Volumes/T7/Obentech/fundingRateData/binance`
  - Bybit：`/Volumes/T7/Obentech/fundingRateData/bybit`
- 工具函数参考：`/Users/rayxu/Documents/Obentech_code/COMMON_UTILS/src/common_utils/utils_Sep.py`

已确认文件存在：
- `/Volumes/T7/Obentech/fundingRateData/binance/POWERUSDT.csv`
- `/Volumes/T7/Obentech/fundingRateData/bybit/POWERUSDT.csv`

## 3. 当前约束与假设
- 暂时没有逐笔成交（trade prints）和逐笔订单事件（order add/cancel）数据。
- 目前有 100ms 深度切片，可用于重建可交易价差与冲击成本。
- `mark/index/premium` 大概率已有，但需确认具体字段名与时间戳列。
- funding interval 先按“稳定不变”处理，但仍要在样本中做一次自动校验。

## 4. 从现有代码得到的关键字段口径（utils_Sep.py）
从 `utils_Sep.py` 可见，funding 相关逻辑默认依赖以下列名：
- Binance：`NextFundingTime`、`FundingRate`
- Bybit/OKX：`FundingTime`、`FundingRate`

并且已有以下基础能力可直接借用：
- 跨所深度对齐（`merge_asof`，100ms 容差）
- 基础价差字段：
  - `sp_open = market2_bid_price1 - market1_ask_price1`
  - `sp_close = market2_ask_price1 - market1_bid_price1`
  - `sr_open = sp_open / market1_ask_price1`
  - `sr_close = sp_close / market1_bid_price1`
- funding 对齐与跨 interval 聚合（`analyze_funding_rate_diff_v2` / `load_funding_diffs`）

## 5. 本次事件要回答的 3 个问题
1. 倒挂形成原因：机制差异（费率计算窗口、标记价格、cap/floor、结算节奏）还是可疑操盘？
2. 在没有逐笔的情况下，能否通过盘口/mark/index/premium 判断“操盘概率”？
3. 倒挂刚出现时，能否识别后续恶化并触发快速减仓/跑路？

## 6. 最小可落地数据清单（按优先级）
`P0`（已有或应可直接拿到）
- 双所 100ms 深度（至少 top5，最好 top20）
- 双所 funding 历史（含 FundingTime / NextFundingTime）
- 双所 mark/index/premium 序列（时间戳精度 >= 1s）

`P1`（强烈建议补齐）
- OI（open interest）时间序列
- 强平量/强平笔数（long/short liquidation）
- 合约参数快照（tick、lot、最小下单量、费率上限下限）

`P2`（未来提升识别精度）
- 逐笔成交（trade）
- 订单级增删改（L2 incremental）

## 7. 分析框架（无逐笔版本）
## 7.1 时间轴与事件窗
- 事件中心：`2026-02-23 23:00:00`（Asia/Shanghai）
- 建议事件窗：`[22:30, 00:30]`（可扩展到前后 2 小时）
- 所有数据统一到 UTC 毫秒，再映射北京时间做可视化。

## 7.2 先做“机制可解释分解”
对每个时间点计算：
- 可交易即时价差（入场/平仓）`sr_open/sr_close`
- 未来一个 funding 窗口可收资金费（按双方 FundingRate 和 interval 对齐）
- 预估冲击成本（由 100ms 深度估算，不用 mid 幻觉）

核心净边际：
- `ExpectedEdge = FundingCarryExpected - EntryBasis - ImpactCost - Fees`

若 `ExpectedEdge < 0`，即使 funding 方向正确，也属于“坏倒挂”，不应继续加仓。

## 7.3 可疑操盘信号（无逐笔时的替代）
没有逐笔时可用以下代理指标：
- `mark/index` 偏离异常：某所 mark 与指数价偏离突然放大，且领先他所
- 盘口失衡突变：bid/ask 深度比短时失真并快速回归
- 冲击成本突升：同等名义金额的吃单成本在 1-3 分钟内倍增
- 基差跳变后快速回落：疑似“窗口拉抬/打压”后回补

说明：这些信号只能给“可疑度”，不能替代订单级证据。

## 7.4 极短时恶化预警与跑路规则
建议三类并行触发器：
- 触发器 A（边际崩塌）：`ExpectedEdge` 连续 N 个点为负（N=5~10）
- 触发器 B（价差恶化）：`basis_zscore > 3` 且 `d(basis)/dt` 持续同向
- 触发器 C（流动性塌陷）：冲击成本短时上升超过阈值（如 +80%）

执行建议：
- 分档减仓（例如 50% -> 30% -> 20%），避免在薄盘口一次性冲击。

## 8. 需要你补充确认的字段（下一步）
请确认以下字段是否在现有文件中：
- Binance/Bybit funding csv 是否包含：
  - `MarkPrice` / `IndexPrice` / `Premium`（或等价列名）
  - 时间戳字段名（毫秒 or 秒）
- 深度文件的字段命名是否与 `utils_Sep.py` 兼容：
  - `bid_price1, bid_size1, ask_price1, ask_size1`（及更多档位）
- 事件窗是否只看 Bybit-BN，还是同时做 OKX-BN 对照组

## 9. 产出物约定
后续可以固定输出三份结果：
1. `POWERUSDT_event_features_20260223.parquet`
2. `POWERUSDT_inversion_diagnosis_20260223.csv`
3. `POWERUSDT_inversion_report_20260223.md`

---
备注：当前外接盘目录枚举较慢，建议优先“按已知文件名直接读取”，避免全目录 `ls/find` 扫描导致阻塞。
