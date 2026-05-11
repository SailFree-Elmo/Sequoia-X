"""A 股场内 ETF 多因子横截面选股：流动性、趋势、动量与回撤约束，综合打分排序。

硬筛：20 日均成交额、趋势（收盘>MA20>MA60）、中期动量为正、短期涨幅上限、相对 60 日高回撤上限。
软打分：在通过硬筛的横截面上，对 log(20 日均额)、20 日收益、60 日收益做分位秩加权求和，按得分降序输出。
"""

from __future__ import annotations

import math

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class EtfMultiFactorStrategy(BaseStrategy):
    """场内 ETF 多因子横截面策略。"""

    webhook_key: str = "etf_multi_factor"
    _MIN_BARS: int = 60

    def run(self) -> list[str]:
        min_turn20 = self.settings.etf_mf_min_turnover_20d
        max_5d_pct = self.settings.etf_mf_max_5d_return_pct
        max_dd60 = self.settings.etf_mf_max_drawdown_from_60d_high
        max_out = self.settings.etf_mf_max_results
        w_liq = self.settings.etf_mf_weight_liquidity
        w_m20 = self.settings.etf_mf_weight_mom20
        w_m60 = self.settings.etf_mf_weight_mom60

        symbols = self.engine.get_local_symbols()
        rows: list[dict[str, float | str]] = []

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS + 5:
                    continue

                c = df["close"].astype(float)
                turnover = df["turnover"].astype(float)

                ma20 = c.rolling(20).mean()
                ma60 = c.rolling(60).mean()

                last = df.iloc[-1]
                c0 = float(last["close"])
                if c0 <= 0 or math.isnan(c0):
                    continue

                ma20v = float(ma20.iloc[-1])
                ma60v = float(ma60.iloc[-1])
                if math.isnan(ma20v) or math.isnan(ma60v) or ma60v <= 0:
                    continue

                if not (c0 > ma20v > ma60v):
                    continue

                turn20 = float(turnover.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                c_5 = float(c.iloc[-6])
                c_20 = float(c.iloc[-21])
                c_60 = float(c.iloc[-61])
                r5 = (c0 / c_5) - 1.0 if c_5 > 0 else -1.0
                r20 = (c0 / c_20) - 1.0 if c_20 > 0 else -1.0
                r60 = (c0 / c_60) - 1.0 if c_60 > 0 else -1.0

                if r20 <= 0 or r60 <= 0:
                    continue
                if r5 > max_5d_pct:
                    continue

                high60 = float(df["high"].iloc[-60:].max())
                if high60 <= 0:
                    continue
                dd_from_high = (high60 - c0) / high60
                if dd_from_high > max_dd60:
                    continue

                rows.append(
                    {
                        "symbol": symbol,
                        "log_turn20": math.log1p(max(turn20, 0.0)),
                        "r20": r20,
                        "r60": r60,
                    }
                )
            except Exception as exc:
                logger.warning(f"[{symbol}] EtfMultiFactorStrategy 计算失败：{exc}")
                continue

        if not rows:
            logger.info("EtfMultiFactorStrategy 选出 0 只 ETF")
            return []

        panel = pd.DataFrame(rows)
        n = len(panel)
        panel["z_liq"] = panel["log_turn20"].rank(pct=True)
        panel["z_m20"] = panel["r20"].rank(pct=True)
        panel["z_m60"] = panel["r60"].rank(pct=True)
        panel["score"] = w_liq * panel["z_liq"] + w_m20 * panel["z_m20"] + w_m60 * panel["z_m60"]
        panel = panel.sort_values("score", ascending=False).head(max_out)

        out = panel["symbol"].astype(str).tolist()
        logger.info(f"EtfMultiFactorStrategy 选出 {len(out)} 只 ETF（横截面 n={n}）")
        return out
