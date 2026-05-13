# Sequoia-X: 王者回归 | The King Returns

> A 股场内 ETF 量化筛选系统 V2 | A-Share Listed ETF Screening System V2

---

## 简介 | Introduction

Sequoia-X V2 面向 **A 股市场场内 ETF**（baostock `type=5`），基于 OOP、向量化计算与增量日 K 更新，可在每日收盘后自动筛选并推送至飞书。

数据层使用 [baostock](http://baostock.com)（免费、无需注册、无限流）拉取历史及增量日 K（后复权），存储于本地 SQLite。

**飞书合并日报：** 日常模式跑完全部策略后 **只发一条** 卡片：可选 **昨日推荐表现**（上一期保存的综合 Top 在次一交易日的「开盘买」「收盘买」两种涨跌幅及等权均值，依赖本地已有下一根日 K）；下方为 **综合推荐 Top N**（命中策略数降序，平局用 Borda 与当日成交额）。各策略命中明细见运行日志 `[digest明细]`。推荐列表按全局最新交易日 `asof_date` 写入表 `digest_top_picks`，请勿删库以免丢失对照历史。Webhook：`FEISHU_WEBHOOK_URL`，可选 `STRATEGY_WEBHOOK_DIGEST`。推荐条数见 `FEISHU_DIGEST_TOP_N`。

**标的池切换说明：** 若曾使用旧版股票库（如 `data/sequoia_v2.db`），请改用新默认路径 `data/etf_sequoia.db`（或通过环境变量 `DB_PATH` 指定），删除旧库或换新路径后重新执行 `python main.py --backfill`。

---

## 两种运行模式

```bash
python main.py               # 日常模式：增量补数据 + 跑策略 + 飞书合并日报（一条）
python main.py --backfill    # 回填模式：全市场 ETF 历史 K 线灌库
```

---

## 内置策略 | Strategies

| 策略类名 | 说明 |
|---|---|
| **MaVolumeStrategy** | 均线金叉 + 放量（倍数见配置 `ma_volume_surge_multiplier`） |
| **TurtleTradeStrategy** | 海龟突破：20 日新高 + 成交额阈值 + 阳线过滤，按成交额排序 |
| **HighTightFlagStrategy** | 高窄旗形整理（动量阈值见 `high_tight_momentum_ratio`） |
| **EtfStrongPullbackStrategy** | 强势日（默认约 3% 涨幅）后放量阴线回踩 |
| **EtfUptrendSharpDropStrategy** | 上升趋势中放量单日大跌（幅度见 `sharp_drop_pct`） |
| **RpsBreakoutStrategy** | ETF 池内 RPS 动量 + 阶段高点突破 |
| **EtfDualMaTrendStrategy** | 连续多日 MA20>MA60 且收盘站上 MA20（默认关闭，可配置启用） |
| **EtfMultiFactorStrategy** | 多因子横截面：流动性 + 趋势 + 动量 + 回撤硬筛，当日样本内分位秩加权打分（权重见 `etf_mf_weight_*`） |
| **EtfTrendFollowStrategy** | 趋势跟随：收盘>MA20>MA60、MA20 上行、20/60 日动量为正、流动性与短期涨幅过滤，按价格相对 MA20 强度排序 |
| **StrongTrendLowChaseStrategy** | 强趋势低追高隔夜：趋势多头 + 不过热 + 收盘质量过滤 |
| **AdxMaRegimeTrendStrategy** | ADX 趋势强度 + MA20/MA60 多头 + ATR 风险过滤 |
| **VolumeContractionBreakoutStrategy** | 缩量收敛后放量突破，过滤假突破 |
| **IndustryRelativeStrengthRotationStrategy** | 行业/主题分桶相对强弱轮动（先择强组，再选组内强势ETF） |
| **NewsSentimentBreadthStrategy** | 消息面情绪扩散策略（读取 `NEWS_SIGNAL_PATH` JSON，默认关闭） |
| **DualMomentumRotationStrategy** | 双动量轮动：相对动量（20/60日）+ 绝对动量（站上MA60） |
| **TrendStabilityMomentumStrategy** | 趋势稳健动量：年化斜率 × R²，过滤伪动量 |
| **LowVolMomentumBlendStrategy** | 低波动动量融合：动量分位 + 波动惩罚，偏高胜率 |

阈值可在 [sequoia_x/core/config.py](sequoia_x/core/config.py) 的 `Settings` 中通过环境变量覆盖（如 `TURTLE_MIN_TURNOVER`、`STRONG_DAY_PCT` 等）。

系统会在每日运行时先计算市场状态（`risk_on/risk_off`），`risk_off` 默认关闭反转类策略并对各策略分组权重进行调整。

---

## 快速开始 | Quick Start

### 环境要求

- Python >= 3.10

### 1. 安装依赖

```bash
uv sync
# 或
pip install .
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填写飞书 Webhook URL；可按需设置 DB_PATH=data/etf_sequoia.db
```

### 3. 首次回填历史数据

```bash
python main.py --backfill
```

耗时取决于 ETF 数量与网络（显著少于全市场股票回填）。

### 4. 日常运行

```bash
python main.py
```

建议配合 crontab 每个交易日收盘后执行：

```cron
15 19 * * 1-5 cd /root/Sequoia-X && .venv/bin/python main.py >> log.txt 2>&1
```

---

## 目录结构 | Project Structure

```
Sequoia-X/
├── main.py
├── pyproject.toml
├── .env.example
├── data/                        # SQLite（默认 etf_sequoia.db，运行时生成）
├── sequoia_x/
│   ├── core/config.py
│   ├── data/engine.py           # ETF 列表筛选 + 回填 + 增量同步
│   │   └── news_adapter.py      # 消息面信号适配（JSON）
│   ├── strategy/
│   │   ├── base.py
│   │   ├── turtle_trade.py
│   │   ├── ma_volume.py
│   │   ├── high_tight_flag.py
│   │   ├── limit_up_shakeout.py # EtfStrongPullbackStrategy
│   │   ├── uptrend_limit_down.py # EtfUptrendSharpDropStrategy
│   │   ├── rps_breakout.py
│   │   ├── etf_dual_ma_trend.py
│   │   ├── etf_multi_factor.py
│   │   ├── etf_trend_follow.py
│   │   ├── strong_trend_low_chase.py
│   │   ├── market_regime_filter.py
│   │   ├── adx_ma_regime_trend.py
│   │   ├── volume_contraction_breakout.py
│   │   ├── industry_relative_strength_rotation.py
│   │   ├── news_sentiment_breadth.py
│   │   ├── dual_momentum_rotation.py
│   │   ├── trend_stability_momentum.py
│   │   └── low_vol_momentum_blend.py
│   └── notify/
│       ├── feishu.py          # 飞书推送（含 send_digest）
│       ├── digest.py          # 合并计分与 Top N
│       └── yesterday_perf.py  # 昨日推荐涨跌幅
└── tests/
```

---

## 数据说明

- **数据源**：[baostock](http://baostock.com)
- **标的**：沪深场内 ETF（`query_stock_basic` 中 `type=5` 且上市）
- **复权方式**：后复权（hfq）
- **存储**：默认 `data/etf_sequoia.db`

---

## 许可证 | License

MIT
