"""趋势稳健动量：年化斜率 × R²。"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class TrendStabilityMomentumStrategy(BaseStrategy):
    webhook_key: str = "trend_stability"
    _MIN_BARS: int = 100

    @staticmethod
    def _slope_r2(log_prices: np.ndarray) -> tuple[float, float]:
        n = len(log_prices)
        if n < 5:
            return 0.0, 0.0
        x = np.arange(n, dtype=float)
        # 近期权重大一点，降低滞后
        w = np.linspace(1.0, 2.0, n, dtype=float)
        xw = np.average(x, weights=w)
        yw = np.average(log_prices, weights=w)
        cov = np.average((x - xw) * (log_prices - yw), weights=w)
        var = np.average((x - xw) ** 2, weights=w)
        if var <= 0:
            return 0.0, 0.0
        slope = cov / var
        yhat = yw + slope * (x - xw)
        sst = np.sum((log_prices - np.mean(log_prices)) ** 2)
        sse = np.sum((log_prices - yhat) ** 2)
        r2 = 1.0 - sse / sst if sst > 0 else 0.0
        return float(slope), float(max(0.0, min(1.0, r2)))

    def run(self) -> list[str]:
        lookback = max(20, int(self.settings.tsm_lookback_days))
        min_turn20 = self.settings.tsm_min_turnover_20d
        max_r5 = self.settings.tsm_max_5d_return_pct
        max_out = max(1, int(self.settings.tsm_max_results))

        symbols = self.engine.get_local_symbols()
        rows: list[dict[str, float | str]] = []
        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < max(self._MIN_BARS, lookback + 65):
                    continue
                c = df["close"].astype(float)
                to = df["turnover"].astype(float)
                ma60 = c.rolling(60).mean()

                c0 = float(c.iloc[-1])
                ma60v = float(ma60.iloc[-1])
                if pd.isna(ma60v) or c0 <= ma60v:
                    continue

                c5 = float(c.iloc[-6])
                if c5 <= 0:
                    continue
                r5 = c0 / c5 - 1.0
                if r5 > max_r5:
                    continue

                turn20 = float(to.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                tail = c.iloc[-lookback:].to_numpy(dtype=float)
                if (tail <= 0).any():
                    continue
                slope, r2 = self._slope_r2(np.log(tail))
                if slope <= 0:
                    continue
                annual_slope = math.exp(slope * 252.0) - 1.0
                score = annual_slope * r2
                rows.append({"symbol": symbol, "score": score, "r2": r2, "turn20": turn20})
            except Exception as exc:
                logger.warning(f"[{symbol}] TrendStabilityMomentumStrategy 计算失败：{exc}")
                continue

        if not rows:
            logger.info("TrendStabilityMomentumStrategy 选出 0 只 ETF")
            return []
        panel = pd.DataFrame(rows)
        panel["rank"] = panel["score"].rank(pct=True) * 0.8 + panel["turn20"].rank(pct=True) * 0.2
        panel = panel.sort_values("rank", ascending=False).head(max_out)
        out = panel["symbol"].astype(str).tolist()
        logger.info(f"TrendStabilityMomentumStrategy 选出 {len(out)} 只 ETF")
        return out
