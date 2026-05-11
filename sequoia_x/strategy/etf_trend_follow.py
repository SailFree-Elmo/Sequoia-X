"""ETF 趋势跟随：均线多头排列 + MA20 上行斜率 + 动量与流动性过滤，按价格相对 MA20 强度排序。"""

from __future__ import annotations

import math

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class EtfTrendFollowStrategy(BaseStrategy):
    """趋势跟随：顺势持仓池，避免下行中的反弹。"""

    webhook_key: str = "etf_trend_follow"
    _MIN_BARS: int = 65

    def run(self) -> list[str]:
        min_turn20 = self.settings.etf_tf_min_turnover_20d
        max_5d = self.settings.etf_tf_max_5d_return_pct
        max_out = self.settings.etf_tf_max_results

        symbols = self.engine.get_local_symbols()
        rows: list[dict[str, float | str]] = []

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue

                c = df["close"].astype(float)
                turnover = df["turnover"].astype(float)
                ma20 = c.rolling(20).mean()
                ma60 = c.rolling(60).mean()

                c0 = float(c.iloc[-1])
                ma20v = float(ma20.iloc[-1])
                ma60v = float(ma60.iloc[-1])
                ma20_prev = float(ma20.iloc[-6])

                if c0 <= 0 or math.isnan(ma20v) or math.isnan(ma60v) or ma60v <= 0:
                    continue
                if not (c0 > ma20v > ma60v):
                    continue
                if ma20v <= ma20_prev:
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
                if r5 > max_5d:
                    continue

                strength = (c0 / ma20v) - 1.0
                rows.append({"symbol": symbol, "strength": strength})
            except Exception as exc:
                logger.warning(f"[{symbol}] EtfTrendFollowStrategy 计算失败：{exc}")
                continue

        if not rows:
            logger.info("EtfTrendFollowStrategy 选出 0 只 ETF")
            return []

        panel = pd.DataFrame(rows).sort_values("strength", ascending=False).head(max_out)
        out = panel["symbol"].astype(str).tolist()
        logger.info(f"EtfTrendFollowStrategy 选出 {len(out)} 只 ETF")
        return out
