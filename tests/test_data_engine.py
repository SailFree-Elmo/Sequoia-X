"""数据引擎属性测试（PostgreSQL）。"""

import os
from datetime import date

import psycopg
import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine


def _test_database_url() -> str:
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("未设置 TEST_DATABASE_URL，跳过 PostgreSQL 集成测试")
    return url


# Property 4: (symbol, date) 唯一约束防止重复写入
@given(
    symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"),
    trade_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2025, 12, 31)),
)
@h_settings(max_examples=30, deadline=None)
def test_unique_symbol_date_constraint(symbol: str, trade_date: date) -> None:
    """相同 (symbol, date) upsert 两次后，数据库中该组合记录数应为 1。"""
    settings = Settings(
        database_url=_test_database_url(),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    row = (symbol, str(trade_date), 10.0, 11.0, 9.0, 10.5, 1000.0, 10500.0)

    with psycopg.connect(engine.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_daily (symbol, date, open, high, low, close, volume, turnover)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE SET close = EXCLUDED.close
                """,
                row,
            )
            cur.execute(
                """
                INSERT INTO stock_daily (symbol, date, open, high, low, close, volume, turnover)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO UPDATE SET close = EXCLUDED.close
                """,
                row,
            )
            cur.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol=%s AND date=%s",
                (symbol, str(trade_date)),
            )
            count = cur.fetchone()[0]
        conn.commit()
    assert count == 1
