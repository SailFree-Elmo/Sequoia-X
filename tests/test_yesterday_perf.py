"""昨日推荐收益率计算与 digest_top_picks 存取。"""

import os

import psycopg
import pytest

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.yesterday_perf import compute_pick_followthrough, format_pct


def _test_database_url() -> str:
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("未设置 TEST_DATABASE_URL，跳过 PostgreSQL 集成测试")
    return url


def _insert_bars(engine: DataEngine, symbol: str, rows: list[dict]) -> None:
    with psycopg.connect(engine.database_url) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO stock_daily (symbol, date, open, high, low, close, volume, turnover)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, date) DO UPDATE SET close = EXCLUDED.close
                    """,
                    (
                        symbol,
                        row["date"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        row["turnover"],
                    ),
                )
        conn.commit()


def test_compute_pick_followthrough_open_open() -> None:
    """开盘买 O→O = (再下一日开盘 / 次日开盘 - 1)。"""
    settings = Settings(
        database_url=_test_database_url(),
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
                "date": "2024-06-03",
                "open": 9.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1e6,
                "turnover": 1e7,
            },
            {
                "date": "2024-06-04",
                "open": 10.0,
                "high": 11.0,
                "low": 9.9,
                "close": 10.5,
                "volume": 1e6,
                "turnover": 1e7,
            },
            {
                "date": "2024-06-05",
                "open": 10.4,
                "high": 11.0,
                "low": 10.0,
                "close": 10.6,
                "volume": 1e6,
                "turnover": 1e7,
            },
        ],
    )
    rows, avg_o = compute_pick_followthrough(
        engine, "2024-06-04", "2024-06-05", [sym], max_rows=10
    )
    assert len(rows) == 1
    r = rows[0]
    assert r.code == sym
    assert abs((r.pct_open_buy or 0) - 4.0) < 0.01
    assert avg_o is not None and abs(avg_o - 4.0) < 0.01


def test_format_pct() -> None:
    assert "—" in format_pct(None)
    assert "+1.23%" == format_pct(1.234)


def test_save_and_load_digest_top_picks() -> None:
    settings = Settings(
        database_url=_test_database_url(),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/h",
    )
    engine = DataEngine(settings)
    engine.save_digest_top_picks(
        "2024-01-02",
        ["111111", "222222"],
        pick_rows=[
            {"code": "111111", "vote_count": 3, "vote_score": 4.5},
            {"code": "222222", "vote_count": 1, "vote_score": 1.25},
        ],
    )
    engine.save_digest_top_picks("2024-01-05", ["333333"])
    d, codes, stats = engine.load_digest_top_picks_strictly_before("2024-01-05")
    assert d == "2024-01-02"
    assert codes == ["111111", "222222"]
    assert stats == {"111111": (3, 4.5), "222222": (1, 1.25)}
    d2, c2, s2 = engine.load_digest_top_picks_strictly_before("2024-01-02")
    assert d2 is None and c2 == [] and s2 == {}

    d3, c3, s3 = engine.load_digest_top_picks_second_latest()
    assert d3 == "2024-01-02"
    assert c3 == ["111111", "222222"]
    assert s3 == {"111111": (3, 4.5), "222222": (1, 1.25)}
