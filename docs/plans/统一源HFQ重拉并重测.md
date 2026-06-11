---
name: 统一源HFQ重拉回测
overview: 按你确认的口径，使用 baostock + 后复权(hfq) 统一重拉 2025~2026 数据，消除跨源拼接影响后重跑 2026 回测并输出新报告。
todos:
  - id: pull-unified-hfq
    content: 使用 baostock(hfq) 重拉并覆盖 2025-01-01~2026-05-13 区间数据
    status: pending
  - id: validate-continuity
    content: 校验重拉后数据覆盖与跨年连续性，确认无跨源口径混入
    status: pending
  - id: rerun-backtest
    content: 按原口径重跑 2026 回测并生成新的 results md 报告
    status: pending
  - id: compare-reports
    content: 对比新旧报告关键指标并给出结论（重点 2026-05）
    status: pending
---

# 统一源 HFQ 重拉并重测计划

## 目标

以 `baostock + hfq` 作为唯一数据口径，重建 `2025-01-01` 到 `2026-05-13` 的 ETF 日线，随后按同一回测规则重跑 2026 收益，产出可对比的新报告。

## 实施步骤

- 在 [sequoia_x/data/engine.py](../../sequoia_x/data/engine.py) 复用现有 `adjustflag="1"`（后复权）抓取逻辑，避免再混用 `yfinance`。
- 先做数据重拉策略：对 `stock_daily` 中 `2025-01-01`~`2026-05-13` 区间按 `symbol+date` 执行幂等 `upsert` 覆盖，确保该窗口内全部记录来自同一源同一复权方式。
- 重拉后做一致性校验（仅检查，不改策略）：
  - 覆盖标的数、交易日覆盖率、关键边界日（`2025-12-31` 与 `2026-01-05`）连续性。
  - 极端跳变标的抽样，确认是否仍有口径异常。
- 使用与当前报告一致的回测口径重跑（Top5 取前2、O->O、双边手续费万0.5、缺开盘价保留现金），生成新报告文件到 `results`。
- 输出三份结果对照：旧报告、新报告、差异摘要（重点看 2026-05 月收益、累计收益、最大回撤、成交次数）。

## 关键文件

- 数据抓取与入库主逻辑：[sequoia_x/data/engine.py](../../sequoia_x/data/engine.py)
- 配置与环境读取：[sequoia_x/core/config.py](../../sequoia_x/core/config.py)
- 报告输出目录：[results](../../results)

## 交付物

- 一份统一口径新报告（新增 md 文件，不覆盖历史文件）。
- 一份简短差异说明（明确 `2026-05` 是否仍接近 `-27%`，以及原因）。
