"""缩量收敛后放量突破策略。"""

from __future__ import annotations

import math

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class VolumeContractionBreakoutStrategy(BaseStrategy):
    webhook_key: str = "volume_contraction"
    _MIN_BARS: int = 80

    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        selected: list[tuple[str, float]] = []
        cw = max(10, int(self.settings.vcb_contraction_window))
        bw = max(cw + 5, int(self.settings.vcb_breakout_window))
        max_range_ratio = self.settings.vcb_contraction_max_range_ratio
        vol_mult = self.settings.vcb_volume_breakout_multiplier
        min_turn20 = self.settings.vcb_min_turnover_20d
        max_5d = self.settings.vcb_max_5d_return_pct
        min_close_pos = self.settings.vcb_min_close_position_ratio

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue
                c = df["close"].astype(float)
                h = df["high"].astype(float)
                l = df["low"].astype(float)
                v = df["volume"].astype(float)
                t = df["turnover"].astype(float)

                recent_h = float(h.iloc[-cw:].max())
                recent_l = float(l.iloc[-cw:].min())
                if recent_l <= 0:
                    continue
                range_ratio = recent_h / recent_l - 1.0
                if range_ratio > max_range_ratio:
                    continue

                breakout_h = float(h.iloc[-bw:-1].max())
                c0 = float(c.iloc[-1])
                if c0 <= breakout_h:
                    continue

                vol_ma20 = float(v.iloc[-21:-1].mean())
                if vol_ma20 <= 0:
                    continue
                if float(v.iloc[-1]) < vol_ma20 * vol_mult:
                    continue

                c5 = float(c.iloc[-6])
                if c5 <= 0:
                    continue
                r5 = c0 / c5 - 1.0
                if r5 > max_5d:
                    continue

                day_range = float(h.iloc[-1] - l.iloc[-1])
                if day_range <= 0:
                    continue
                close_pos_ratio = float(c0 - l.iloc[-1]) / day_range
                if close_pos_ratio < min_close_pos:
                    continue

                turn20 = float(t.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                score = (
                    (c0 / breakout_h - 1.0)
                    + (float(v.iloc[-1]) / vol_ma20 - 1.0)
                    + close_pos_ratio * 0.2
                )
                if math.isnan(score):
                    continue
                selected.append((symbol, score))
            except Exception as exc:
                logger.warning(f"[{symbol}] VolumeContractionBreakoutStrategy 计算失败：{exc}")
                continue

        selected.sort(key=lambda x: x[1], reverse=True)
        out = [s for s, _ in selected]
        logger.info(f"VolumeContractionBreakoutStrategy 选出 {len(out)} 只 ETF")
        return out
