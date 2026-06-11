"""新增策略与门控层测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.strategy.market_regime_filter import MarketRegimeFilter
from sequoia_x.strategy.news_sentiment_breadth import NewsSentimentBreadthStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy


class StubEngine:
    def __init__(self, data: dict[str, pd.DataFrame]) -> None:
        self._data = data

    def get_local_symbols(self) -> list[str]:
        return list(self._data.keys())

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        return self._data[symbol]


def _make_ohlcv(closes: list[float], *, volume: float = 1_000_000.0, turnover: float = 2e7) -> pd.DataFrame:
    rows = []
    for i, c in enumerate(closes):
        rows.append(
            {
                "date": f"2024-01-{(i % 28) + 1:02d}",
                "open": c * 0.99,
                "high": c * 1.01,
                "low": c * 0.98,
                "close": c,
                "volume": volume,
                "turnover": turnover,
            }
        )
    return pd.DataFrame(rows)


def test_rps_breakout_uses_strict_breakout() -> None:
    settings = Settings(
        database_url="postgresql://user:pass@127.0.0.1:5433/test_db",
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
        rps_period=20,
        rps_threshold=50,
        rps_breakout_buffer=0.0,
        rps_min_turnover_20d=1.0,
        rps_max_atr_ratio=1.0,
    )

    # A: 最后一日创新高；B: 未创新高
    a_close = [10.0 + i * 0.1 for i in range(139)] + [25.0]
    b_close = [10.0 + i * 0.1 for i in range(139)] + [20.0]
    data = {
        "510300": _make_ohlcv(a_close, turnover=3e7),
        "159919": _make_ohlcv(b_close, turnover=3e7),
    }
    engine = StubEngine(data)
    out = RpsBreakoutStrategy(engine=engine, settings=settings).run()
    assert "510300" in out


def test_market_regime_filter_detects_risk_off() -> None:
    settings = Settings(
        database_url="postgresql://user:pass@127.0.0.1:5433/test_db",
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
        regime_benchmark_symbols="510300,159915",
        regime_ma_window=20,
        regime_strength_min_ratio=0.8,
        regime_breadth_min_ratio=0.8,
    )
    falling = [20.0 - i * 0.1 for i in range(60)]
    engine = StubEngine(
        {
            "510300": _make_ohlcv(falling),
            "159915": _make_ohlcv(falling),
            "159919": _make_ohlcv(falling),
        }
    )
    regime = MarketRegimeFilter(engine=engine, settings=settings).detect()
    assert regime.regime == "risk_off"


def test_news_sentiment_strategy_reads_json(tmp_path: Path) -> None:
    p = tmp_path / "news_signals.json"
    p.write_text(
        json.dumps(
            [
                {"symbol": "510300", "date": "2026-05-12", "sentiment": 0.6, "heat": 0.4},
                {"symbol": "510300", "date": "2026-05-13", "sentiment": 0.7, "heat": 0.8},
                {"symbol": "159919", "date": "2026-05-13", "sentiment": -0.2, "heat": 0.2},
            ]
        ),
        encoding="utf-8",
    )
    settings = Settings(
        database_url="postgresql://user:pass@127.0.0.1:5433/test_db",
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
        news_signal_path=str(p),
        news_lookback_days=1000,
        news_sentiment_threshold=0.2,
        news_heat_accel_threshold=0.0,
    )
    engine = StubEngine({"510300": _make_ohlcv([1.0] * 70)})
    out = NewsSentimentBreadthStrategy(engine=engine, settings=settings).run()
    assert out == ["510300"]