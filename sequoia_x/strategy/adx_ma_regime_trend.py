"""ADX 趋势强度 + 双均线结构 + ATR 风险过滤。"""

from __future__ import annotations

import math

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class AdxMaRegimeTrendStrategy(BaseStrategy):
    webhook_key: str = "adx_trend"
    _MIN_BARS: int = 80

    def _calc_adx(self, df: pd.DataFrame, period: int) -> pd.Series:
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        close = df["close"].astype(float)
        prev_close = close.shift(1)

        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

        tr = pd.concat(
            [
                (high - low),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(period).mean()

        plus_di = 100.0 * (plus_dm.rolling(period).mean() / atr)
        minus_di = 100.0 * (minus_dm.rolling(period).mean() / atr)
        dx = (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, math.nan) * 100.0
        return dx.rolling(period).mean()

    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        selected: list[tuple[str, float]] = []
        period = max(5, int(self.settings.adx_period))
        adx_min = self.settings.adx_threshold
        max_atr_ratio = self.settings.adx_atr_ratio_max
        min_turn20 = self.settings.adx_min_turnover_20d

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue
                c = df["close"].astype(float)
                h = df["high"].astype(float)
                l = df["low"].astype(float)
                ma20 = c.rolling(20).mean()
                ma60 = c.rolling(60).mean()
                adx = self._calc_adx(df, period)
                tr = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(
                    axis=1
                )
                atr = tr.rolling(period).mean()
                turn20 = df["turnover"].astype(float).iloc[-20:].mean()

                c0 = float(c.iloc[-1])
                ma20v = float(ma20.iloc[-1])
                ma60v = float(ma60.iloc[-1])
                adxv = float(adx.iloc[-1])
                atrv = float(atr.iloc[-1])
                if any(math.isnan(v) for v in (ma20v, ma60v, adxv, atrv)):
                    continue
                if not (c0 > ma20v > ma60v):
                    continue
                if adxv < adx_min:
                    continue
                if turn20 < min_turn20:
                    continue
                atr_ratio = atrv / c0 if c0 > 0 else 1.0
                if atr_ratio > max_atr_ratio:
                    continue
                score = adxv * (1.0 + (c0 / ma20v - 1.0))
                selected.append((symbol, score))
            except Exception as exc:
                logger.warning(f"[{symbol}] AdxMaRegimeTrendStrategy 计算失败：{exc}")
                continue

        selected.sort(key=lambda x: x[1], reverse=True)
        out = [s for s, _ in selected]
        logger.info(f"AdxMaRegimeTrendStrategy 选出 {len(out)} 只 ETF")
        return out
