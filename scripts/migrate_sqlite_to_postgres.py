#!/usr/bin/env python3
"""一次性将 SQLite 数据迁移到 PostgreSQL。"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path

import psycopg

CREATE_STOCK_DAILY_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       BIGSERIAL PRIMARY KEY,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     DOUBLE PRECISION,
    high     DOUBLE PRECISION,
    low      DOUBLE PRECISION,
    close    DOUBLE PRECISION,
    volume   DOUBLE PRECISION,
    turnover DOUBLE PRECISION,
    UNIQUE (symbol, date)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

CREATE_DIGEST_SQL = """
CREATE TABLE IF NOT EXISTS digest_top_picks (
    asof_date TEXT PRIMARY KEY,
    codes     TEXT NOT NULL
);
"""

UPSERT_STOCK_DAILY_SQL = """
INSERT INTO stock_daily (symbol, date, open, high, low, close, volume, turnover)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, date) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    turnover = EXCLUDED.turnover
"""

UPSERT_DIGEST_SQL = """
INSERT INTO digest_top_picks (asof_date, codes)
VALUES (%s, %s)
ON CONFLICT (asof_date) DO UPDATE SET
    codes = EXCLUDED.codes
"""


def ensure_target_schema(pg_conn: psycopg.Connection) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(CREATE_STOCK_DAILY_SQL)
        cur.execute(CREATE_INDEX_SQL)
        cur.execute(CREATE_DIGEST_SQL)
    pg_conn.commit()


def migrate_stock_daily(
    sqlite_conn: sqlite3.Connection,
    pg_conn: psycopg.Connection,
    batch_size: int,
) -> int:
    total = 0
    src_cur = sqlite_conn.cursor()
    src_cur.execute(
        "SELECT symbol, date, open, high, low, close, volume, turnover "
        "FROM stock_daily ORDER BY symbol, date"
    )

    with pg_conn.cursor() as cur:
        while True:
            rows = src_cur.fetchmany(batch_size)
            if not rows:
                break
            cur.executemany(UPSERT_STOCK_DAILY_SQL, rows)
            total += len(rows)
            pg_conn.commit()
            print(f"[stock_daily] migrated rows: {total}", flush=True)
    return total


def migrate_digest_top_picks(
    sqlite_conn: sqlite3.Connection,
    pg_conn: psycopg.Connection,
) -> int:
    rows = sqlite_conn.execute(
        "SELECT asof_date, codes FROM digest_top_picks ORDER BY asof_date"
    ).fetchall()
    if not rows:
        return 0

    with pg_conn.cursor() as cur:
        cur.executemany(UPSERT_DIGEST_SQL, rows)
    pg_conn.commit()
    return len(rows)


def count_sqlite(sqlite_conn: sqlite3.Connection, table: str) -> int:
    row = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0]) if row else 0


def count_pg(pg_conn: psycopg.Connection, table: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
    return int(row[0]) if row else 0


def min_max_date_sqlite(sqlite_conn: sqlite3.Connection) -> tuple[str | None, str | None]:
    row = sqlite_conn.execute("SELECT MIN(date), MAX(date) FROM stock_daily").fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def min_max_date_pg(pg_conn: psycopg.Connection) -> tuple[str | None, str | None]:
    with pg_conn.cursor() as cur:
        cur.execute("SELECT MIN(date), MAX(date) FROM stock_daily")
        row = cur.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="迁移 SQLite 到 PostgreSQL")
    parser.add_argument(
        "--sqlite-path",
        default="data/etf_sequoia.db",
        help="SQLite 数据库文件路径",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="PostgreSQL 连接串（默认从 DATABASE_URL 读取）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="stock_daily 分批迁移行数",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url:
        raise ValueError("缺少 --database-url，且环境变量 DATABASE_URL 未设置")

    sqlite_path = Path(args.sqlite_path)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite 文件不存在: {sqlite_path}")

    started = time.time()
    print(f"SQLite source: {sqlite_path}", flush=True)
    print("PostgreSQL target: configured by DATABASE_URL/--database-url", flush=True)

    with sqlite3.connect(sqlite_path) as sqlite_conn, psycopg.connect(args.database_url) as pg_conn:
        ensure_target_schema(pg_conn)

        migrated_stock_daily = migrate_stock_daily(sqlite_conn, pg_conn, args.batch_size)
        migrated_digest = migrate_digest_top_picks(sqlite_conn, pg_conn)

        src_stock_count = count_sqlite(sqlite_conn, "stock_daily")
        src_digest_count = count_sqlite(sqlite_conn, "digest_top_picks")
        dst_stock_count = count_pg(pg_conn, "stock_daily")
        dst_digest_count = count_pg(pg_conn, "digest_top_picks")
        src_min_date, src_max_date = min_max_date_sqlite(sqlite_conn)
        dst_min_date, dst_max_date = min_max_date_pg(pg_conn)

    elapsed = time.time() - started
    print("=== Migration Summary ===", flush=True)
    print(f"migrated stock_daily rows: {migrated_stock_daily}", flush=True)
    print(f"migrated digest_top_picks rows: {migrated_digest}", flush=True)
    print(f"stock_daily count: sqlite={src_stock_count}, pg={dst_stock_count}", flush=True)
    print(f"digest_top_picks count: sqlite={src_digest_count}, pg={dst_digest_count}", flush=True)
    print(
        f"stock_daily date range: sqlite=({src_min_date},{src_max_date}), "
        f"pg=({dst_min_date},{dst_max_date})",
        flush=True,
    )
    print(f"elapsed seconds: {elapsed:.2f}", flush=True)


if __name__ == "__main__":
    main()