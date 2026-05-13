#!/usr/bin/env python3
"""使用 AKShare 回填 2023-2024 ETF 日线到 SQLite."""

from __future__ import annotations

import argparse
import os
import socket
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd


START_DATE = "2023-01-01"
END_DATE = "2024-12-31"


@dataclass
class RunStats:
    processed: int = 0
    success: int = 0
    skipped_empty: int = 0
    failed: int = 0
    inserted_rows: int = 0
    retried: int = 0


def _exchange_prefix(symbol: str) -> str:
    return "sh" if symbol.startswith(("5", "6", "9")) else "sz"


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _normalize_etf_daily(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
        )

    date_col = _pick_column(df, ["日期", "date", "Date"])
    open_col = _pick_column(df, ["开盘", "开盘价", "open", "Open"])
    high_col = _pick_column(df, ["最高", "最高价", "high", "High"])
    low_col = _pick_column(df, ["最低", "最低价", "low", "Low"])
    close_col = _pick_column(df, ["收盘", "收盘价", "close", "Close"])
    volume_col = _pick_column(df, ["成交量", "volume", "Volume"])
    amount_col = _pick_column(df, ["成交额", "amount", "turnover", "Amount"])

    required = [date_col, open_col, high_col, low_col, close_col, volume_col, amount_col]
    if any(col is None for col in required):
        return pd.DataFrame(
            columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
        )

    normalized = pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d"),
            "open": pd.to_numeric(df[open_col], errors="coerce"),
            "high": pd.to_numeric(df[high_col], errors="coerce"),
            "low": pd.to_numeric(df[low_col], errors="coerce"),
            "close": pd.to_numeric(df[close_col], errors="coerce"),
            "volume": pd.to_numeric(df[volume_col], errors="coerce"),
            "turnover": pd.to_numeric(df[amount_col], errors="coerce"),
        }
    )

    normalized = normalized.dropna(subset=["date", "close"])
    normalized = normalized[(normalized["date"] >= START_DATE) & (normalized["date"] <= END_DATE)]
    normalized = normalized[normalized["volume"].fillna(0) > 0]
    normalized = normalized.drop_duplicates(subset=["symbol", "date"], keep="last")
    return normalized


def _fetch_etf_history(symbol: str, max_retries: int, sleep_seconds: float) -> pd.DataFrame:
    prefix = _exchange_prefix(symbol)
    sina_symbol = f"{prefix}{symbol}"
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            raw = ak.fund_etf_hist_sina(symbol=sina_symbol)
            normalized = _normalize_etf_daily(raw, symbol=symbol)
            return normalized
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        if attempt < max_retries - 1:
            time.sleep(sleep_seconds * (attempt + 1))

    if last_error:
        raise last_error
    return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"])


def _load_symbols(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM stock_daily WHERE symbol IS NOT NULL AND symbol != '' ORDER BY symbol"
    ).fetchall()
    return [row[0] for row in rows]


def _upsert_rows(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    records = list(
        df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]
        .itertuples(index=False, name=None)
    )
    conn.executemany(
        "INSERT OR REPLACE INTO stock_daily "
        "(symbol, date, open, high, low, close, volume, turnover) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        records,
    )
    return len(records)


def _run_backfill(
    conn: sqlite3.Connection,
    symbols: list[str],
    max_retries: int,
    sleep_seconds: float,
    workers: int,
    second_round_only_failed: bool = False,
) -> tuple[RunStats, dict[str, str]]:
    stats = RunStats()
    failed_reasons: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_map = {
            pool.submit(
                _fetch_etf_history,
                symbol,
                max_retries,
                sleep_seconds,
            ): symbol
            for symbol in symbols
        }
        for idx, future in enumerate(as_completed(future_map), start=1):
            symbol = future_map[future]
            stats.processed += 1
            try:
                df = future.result()
                if df.empty:
                    stats.skipped_empty += 1
                else:
                    written = _upsert_rows(conn, df)
                    stats.inserted_rows += written
                    stats.success += 1
            except Exception as exc:  # noqa: BLE001
                stats.failed += 1
                failed_reasons[symbol] = str(exc)

            if idx % 50 == 0:
                conn.commit()
                print(
                    f"[{'retry' if second_round_only_failed else 'round1'}] "
                    f"{idx}/{len(symbols)} processed | success={stats.success} "
                    f"empty={stats.skipped_empty} failed={stats.failed} rows={stats.inserted_rows}",
                    flush=True,
                )

    conn.commit()
    return stats, failed_reasons


def _coverage_stats(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        """
        SELECT substr(date, 1, 4) AS year,
               COUNT(*) AS rows,
               COUNT(DISTINCT symbol) AS symbols,
               MIN(date) AS min_date,
               MAX(date) AS max_date
        FROM stock_daily
        WHERE date BETWEEN ? AND ?
        GROUP BY substr(date, 1, 4)
        ORDER BY year
        """,
        (START_DATE, END_DATE),
    ).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(description="回填 2023-2024 ETF 日线到 SQLite")
    parser.add_argument("--db-path", default="data/etf_sequoia.db", help="SQLite 路径")
    parser.add_argument("--max-retries", type=int, default=3, help="单代码最大重试次数")
    parser.add_argument("--sleep-seconds", type=float, default=0.8, help="失败重试基础等待秒数")
    parser.add_argument("--socket-timeout", type=float, default=15.0, help="网络请求超时秒数")
    parser.add_argument("--workers", type=int, default=8, help="并发线程数")
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 个 symbol（0 表示全量）")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    started = datetime.now()
    for key in [
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
    ]:
        os.environ.pop(key, None)
    socket.setdefaulttimeout(args.socket_timeout)
    print(f"Backfill started at: {started.isoformat(timespec='seconds')}", flush=True)
    print(f"Database: {db_path}", flush=True)
    print(f"Target window: {START_DATE} ~ {END_DATE}", flush=True)

    with sqlite3.connect(db_path) as conn:
        symbols = _load_symbols(conn)
        if args.limit > 0:
            symbols = symbols[: args.limit]
        print(f"Loaded symbols from local DB: {len(symbols)}", flush=True)
        if not symbols:
            print("No symbols found, exit.", flush=True)
            return

        round1_stats, failed = _run_backfill(
            conn=conn,
            symbols=symbols,
            max_retries=args.max_retries,
            sleep_seconds=args.sleep_seconds,
            workers=args.workers,
            second_round_only_failed=False,
        )

        round2_stats = RunStats()
        if failed:
            failed_symbols = sorted(failed.keys())
            print(f"Round1 failed symbols: {len(failed_symbols)}. Running second round retry...", flush=True)
            round2_stats, failed_round2 = _run_backfill(
                conn=conn,
                symbols=failed_symbols,
                max_retries=args.max_retries,
                sleep_seconds=args.sleep_seconds,
                workers=args.workers,
                second_round_only_failed=True,
            )
            failed = failed_round2

        ended = datetime.now()
        elapsed = (ended - started).total_seconds()

        print("=== Backfill Summary ===", flush=True)
        print(
            "Round1 => "
            f"processed={round1_stats.processed}, success={round1_stats.success}, "
            f"empty={round1_stats.skipped_empty}, failed={round1_stats.failed}, "
            f"rows={round1_stats.inserted_rows}"
        )
        if round2_stats.processed > 0:
            print(
                "Round2 => "
                f"processed={round2_stats.processed}, success={round2_stats.success}, "
                f"empty={round2_stats.skipped_empty}, failed={round2_stats.failed}, "
                f"rows={round2_stats.inserted_rows}"
            )

        total_rows = round1_stats.inserted_rows + round2_stats.inserted_rows
        print(f"Total upsert rows: {total_rows}", flush=True)
        print(f"Failed final symbols: {len(failed)}", flush=True)
        if failed:
            sample = list(sorted(failed.items()))[:20]
            print("Failed sample (symbol, reason):", flush=True)
            for sym, reason in sample:
                print(f"  - {sym}: {reason[:160]}", flush=True)

        print(f"Elapsed seconds: {elapsed:.1f}", flush=True)

        print("=== Coverage 2023-2024 ===", flush=True)
        coverage = _coverage_stats(conn)
        if not coverage:
            print("No rows in target window.", flush=True)
        else:
            for year, rows, symbols_cnt, min_date, max_date in coverage:
                print(
                    f"year={year}, rows={rows}, distinct_symbol={symbols_cnt}, "
                    f"min_date={min_date}, max_date={max_date}",
                    flush=True,
                )


if __name__ == "__main__":
    main()
