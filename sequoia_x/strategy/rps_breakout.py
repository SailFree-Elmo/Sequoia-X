import math

import pandas as pd

from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


class RpsBreakoutStrategy(BaseStrategy):
    """RPS 极强动量突破策略（在场内 ETF 池内横向排名）。"""

    webhook_key: str = "rps"

    def run(self) -> list[str]:
        period = self.settings.rps_period
        threshold = self.settings.rps_threshold
        breakout_buffer = self.settings.rps_breakout_buffer
        min_turn20 = self.settings.rps_min_turnover_20d
        max_atr_ratio = self.settings.rps_max_atr_ratio

        symbols = self.engine.get_local_symbols()
        features: list[dict[str, float | str]] = []
        min_bars = max(period + 2, 120)
        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < min_bars:
                    continue
                c = df["close"].astype(float)
                h = df["high"].astype(float)
                l = df["low"].astype(float)
                t = df["turnover"].astype(float)

                c0 = float(c.iloc[-1])
                c_shift = float(c.iloc[-(period + 1)])
                if c_shift <= 0:
                    continue
                perf = c0 / c_shift - 1.0

                prior_high = float(h.iloc[-(period + 1):-1].max())
                if prior_high <= 0:
                    continue
                breakout = c0 >= prior_high * (1.0 + breakout_buffer)
                if not breakout:
                    continue

                tr = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(
                    axis=1
                )
                atr14 = float(tr.rolling(14).mean().iloc[-1])
                if math.isnan(atr14) or c0 <= 0:
                    continue
                atr_ratio = atr14 / c0
                if atr_ratio > max_atr_ratio:
                    continue

                turn20 = float(t.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                features.append(
                    {
                        "symbol": symbol,
                        "perf": perf,
                        "turn20": turn20,
                        "atr_ratio": atr_ratio,
                    }
                )
            except Exception as exc:
                logger.warning(f"[{symbol}] RpsBreakoutStrategy 计算失败：{exc}")
                continue

        if not features:
            logger.info("RpsBreakoutStrategy 选出 0 只 ETF")
            return []

        panel = pd.DataFrame(features)
        panel["rps"] = panel["perf"].rank(pct=True) * 100.0
        panel = panel[panel["rps"] >= threshold].copy()
        if panel.empty:
            logger.info("RpsBreakoutStrategy 选出 0 只 ETF")
            return []
        panel["score"] = panel["rps"] * 0.8 + panel["turn20"].rank(pct=True) * 20.0
        panel = panel.sort_values("score", ascending=False)
        out = panel["symbol"].astype(str).tolist()
        logger.info(f"RpsBreakoutStrategy 选出 {len(out)} 只 ETF")
        return out
