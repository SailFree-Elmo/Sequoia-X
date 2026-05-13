"""市场状态门控：根据宽基趋势与ETF广度判断 risk_on/risk_off。"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine

logger = get_logger(__name__)


@dataclass(frozen=True)
class MarketRegime:
    regime: str
    benchmark_strength_ratio: float
    breadth_ratio: float

    @property
    def is_risk_on(self) -> bool:
        return self.regime == "risk_on"


class MarketRegimeFilter:
    """输出市场状态，供其它策略做启停与权重调节。"""

    def __init__(self, engine: DataEngine, settings: Settings) -> None:
        self.engine = engine
        self.settings = settings

    def detect(self) -> MarketRegime:
        ma_window = max(5, int(self.settings.regime_ma_window))
        bench_symbols = [
            s.strip() for s in self.settings.regime_benchmark_symbols.split(",") if s.strip()
        ]
        bench_scores: list[bool] = []
        for symbol in bench_symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < ma_window + 1:
                    continue
                c = df["close"].astype(float)
                ma = c.rolling(ma_window).mean()
                bench_scores.append(bool(c.iloc[-1] > ma.iloc[-1]))
            except Exception:
                continue
        bench_ratio = float(sum(bench_scores) / len(bench_scores)) if bench_scores else 0.0

        symbols = self.engine.get_local_symbols()
        breadth_flags: list[bool] = []
        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < ma_window + 1:
                    continue
                c = df["close"].astype(float)
                ma = c.rolling(ma_window).mean()
                if pd.isna(ma.iloc[-1]):
                    continue
                breadth_flags.append(bool(c.iloc[-1] > ma.iloc[-1]))
            except Exception:
                continue
        breadth_ratio = float(sum(breadth_flags) / len(breadth_flags)) if breadth_flags else 0.0

        risk_on = (
            bench_ratio >= self.settings.regime_strength_min_ratio
            and breadth_ratio >= self.settings.regime_breadth_min_ratio
        )
        regime = "risk_on" if risk_on else "risk_off"
        logger.info(
            "MarketRegime=%s bench_ratio=%.3f breadth_ratio=%.3f",
            regime,
            bench_ratio,
            breadth_ratio,
        )
        return MarketRegime(
            regime=regime,
            benchmark_strength_ratio=bench_ratio,
            breadth_ratio=breadth_ratio,
        )
