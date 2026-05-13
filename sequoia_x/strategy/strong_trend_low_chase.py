"""强趋势低追高隔夜策略：顺势但不过热，强调收盘质量。"""

from __future__ import annotations

import math

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class StrongTrendLowChaseStrategy(BaseStrategy):
    webhook_key: str = "strong_trend_low_chase"
    _MIN_BARS: int = 80

    def run(self) -> list[str]:
        min_turn20 = self.settings.stlc_min_turnover_20d
        max_5d = self.settings.stlc_max_5d_return_pct
        max_dist_ma20 = self.settings.stlc_max_distance_from_ma20
        max_upper_shadow = self.settings.stlc_max_upper_shadow_ratio
        min_close_pos = self.settings.stlc_min_close_position_ratio
        max_results = max(1, int(self.settings.stlc_max_results))

        symbols = self.engine.get_local_symbols()
        rows: list[tuple[str, float]] = []
        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue
                c = df["close"].astype(float)
                h = df["high"].astype(float)
                l = df["low"].astype(float)
                o = df["open"].astype(float)
                to = df["turnover"].astype(float)
                ma20 = c.rolling(20).mean()
                ma60 = c.rolling(60).mean()

                c0 = float(c.iloc[-1])
                ma20v = float(ma20.iloc[-1])
                ma60v = float(ma60.iloc[-1])
                ma20_prev = float(ma20.iloc[-6])
                if any(math.isnan(v) for v in (ma20v, ma60v, ma20_prev)):
                    continue
                if not (c0 > ma20v > ma60v):
                    continue
                if ma20v <= ma20_prev:
                    continue

                turn20 = float(to.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                c5 = float(c.iloc[-6])
                c20 = float(c.iloc[-21])
                c60 = float(c.iloc[-61])
                if c5 <= 0 or c20 <= 0 or c60 <= 0:
                    continue
                r5 = c0 / c5 - 1.0
                r20 = c0 / c20 - 1.0
                r60 = c0 / c60 - 1.0
                if r5 <= 0 or r5 > max_5d:
                    continue
                if r20 <= 0 or r60 <= 0:
                    continue

                dist_ma20 = c0 / ma20v - 1.0
                if dist_ma20 > max_dist_ma20:
                    continue

                day_range = float(h.iloc[-1] - l.iloc[-1])
                if day_range <= 0:
                    continue
                upper_shadow_ratio = float(h.iloc[-1] - c0) / day_range
                close_pos_ratio = float(c0 - l.iloc[-1]) / day_range
                if upper_shadow_ratio > max_upper_shadow:
                    continue
                if close_pos_ratio < min_close_pos:
                    continue
                if c0 < float(o.iloc[-1]):
                    continue

                score = r20 * 0.45 + r60 * 0.35 + close_pos_ratio * 0.15 - dist_ma20 * 0.05
                rows.append((symbol, score))
            except Exception as exc:
                logger.warning(f"[{symbol}] StrongTrendLowChaseStrategy 计算失败：{exc}")
                continue

        rows.sort(key=lambda x: x[1], reverse=True)
        out = [s for s, _ in rows[:max_results]]
        logger.info(f"StrongTrendLowChaseStrategy 选出 {len(out)} 只 ETF")
        return out
