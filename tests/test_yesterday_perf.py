"""昨日推荐收益率计算与 digest_top_picks 存取。"""

import tempfile
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.yesterday_perf import compute_pick_followthrough, format_pct


def _insert_bars(engine: DataEngine, symbol: str, rows: list[dict]) -> None:
    import sqlite3

    df = pd.DataFrame(rows)
    df["symbol"] = symbol
    with sqlite3.connect(engine.db_path) as conn:
        df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")


def test_compute_pick_followthrough_open_and_close_buy() -> None:
    """开盘买 = 当日 (C-O)/O；收盘买 = (次日收/首日收 - 1)。"""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        settings = Settings(
            db_path=str(Path(tmp) / "t.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/h",
        )
        engine = DataEngine(settings)
        sym = "510300"
        _insert_bars(
            engine,
            sym,
            [
                {
                    "date": "2024-06-03", "open": 10.0, "high": 10.5, "low": 9.8,
                    "close": 10.2, "volume": 1e6, "turnover": 1e7,
                },
                {
                    "date": "2024-06-04", "open": 10.0, "high": 11.0, "low": 9.9,
                    "close": 10.5, "volume": 1e6, "turnover": 1e7,
                },
            ],
        )
        rows, avg_o, avg_c = compute_pick_followthrough(
            engine, "2024-06-03", "2024-06-04", [sym], max_rows=10
        )
        assert len(rows) == 1
        r = rows[0]
        assert r.code == sym
        # open buy: 10.5/10.0 - 1 = 5%
        assert abs((r.pct_open_buy or 0) - 5.0) < 0.01
        # close buy: 10.5/10.2 - 1
        assert abs((r.pct_close_buy or 0) - (10.5 / 10.2 - 1) * 100) < 0.01
        assert avg_o is not None and avg_c is not None


def test_format_pct() -> None:
    assert "—" in format_pct(None)
    assert "+1.23%" == format_pct(1.234)


def test_save_and_load_digest_top_picks() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        settings = Settings(
            db_path=str(Path(tmp) / "t.db"),
            start_date="2024-01-01",
            feishu_webhook_url="https://example.com/h",
        )
        engine = DataEngine(settings)
        engine.save_digest_top_picks("2024-01-02", ["111111", "222222"])
        engine.save_digest_top_picks("2024-01-05", ["333333"])
        d, codes = engine.load_digest_top_picks_strictly_before("2024-01-05")
        assert d == "2024-01-02"
        assert codes == ["111111", "222222"]
        d2, c2 = engine.load_digest_top_picks_strictly_before("2024-01-02")
        assert d2 is None and c2 == []

        d3, c3 = engine.load_digest_top_picks_second_latest()
        assert d3 == "2024-01-02"
        assert c3 == ["111111", "222222"]
