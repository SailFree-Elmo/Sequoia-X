"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import json
import sqlite3
from pathlib import Path

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

_CREATE_DIGEST_PICKS_SQL = """
CREATE TABLE IF NOT EXISTS digest_top_picks (
    asof_date TEXT PRIMARY KEY,
    codes     TEXT NOT NULL
);
"""


def _bs_fetch_batch(tasks: list) -> dict:
    """多进程 worker：独立 login，批量拉取 baostock 数据并带重试。"""
    import time
    import baostock as bs

    # 失败时返回完整失败清单，交给主进程统一告警。
    login = bs.login()
    if login.error_code != "0":
        return {
            "rows": [],
            "failed_symbols": [symbol for symbol, *_ in tasks],
            "retry_count": 0,
        }

    rows: list[list[str]] = []
    failed_symbols: list[str] = []
    retry_count = 0
    max_retries = 3

    try:
        for symbol, bs_code, start, end in tasks:
            fetched = False
            for attempt in range(max_retries):
                try:
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=start,
                        end_date=end,
                        frequency="d",
                        adjustflag="1",  # 后复权
                    )
                    if rs.error_code != "0":
                        raise RuntimeError(rs.error_msg)

                    while rs.next():
                        rows.append([symbol] + rs.get_row_data())
                    fetched = True
                    break
                except Exception:
                    retry_count += 1
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)

            if not fetched:
                failed_symbols.append(symbol)
    finally:
        bs.logout()

    return {
        "rows": rows,
        "failed_symbols": failed_symbols,
        "retry_count": retry_count,
    }


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        self._init_db()

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.execute(_CREATE_DIGEST_PICKS_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )
        return df

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将纯数字代码转为 baostock 格式：沪（5/6/9）-> sh，其余 -> sz（含深市 ETF）。"""
        prefix = "sh" if symbol.startswith(("5", "6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    # ── 数据同步 ──

    def sync_today_bulk(self) -> int:
        """多进程并行通过 baostock 拉取增量数据（后复权），幂等写入 SQLite。"""
        from datetime import date, timedelta
        from multiprocessing import Pool

        today_str = date.today().strftime("%Y-%m-%d")

        tasks = []
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol"
            ).fetchall()

        if not rows:
            logger.warning("本地无 ETF 行情数据，请先执行 --backfill")
            return 0

        for symbol, last_date in rows:
            if last_date and last_date >= today_str:
                continue
            start = today_str
            if last_date:
                start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            tasks.append((symbol, self._to_baostock_code(symbol), start, today_str))

        if not tasks:
            logger.info("所有 ETF 已是最新，无需更新")
            return 0

        logger.info(f"需要更新 {len(tasks)} 只 ETF，启动多进程并行拉取...")

        n_workers = min(8, len(tasks))
        chunks = [tasks[i::n_workers] for i in range(n_workers)]

        with Pool(n_workers) as pool:
            batch_results = pool.map(_bs_fetch_batch, chunks)

        all_rows: list[list[str]] = []
        failed_symbols: list[str] = []
        retry_count = 0
        for batch in batch_results:
            all_rows.extend(batch.get("rows", []))
            failed_symbols.extend(batch.get("failed_symbols", []))
            retry_count += int(batch.get("retry_count", 0))

        if not all_rows:
            if failed_symbols:
                logger.warning(
                    "本次增量未写入数据，且有 %d 只 ETF 拉取失败（例如: %s）",
                    len(failed_symbols),
                    ",".join(failed_symbols[:20]),
                )
            else:
                logger.info("无新数据（可能非交易日）")
            return 0

        df = pd.DataFrame(all_rows, columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"])
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df[df["volume"] > 0]
        df = df.drop_duplicates(subset=["symbol", "date"], keep="last")

        count = len(df)
        if count == 0:
            logger.warning("抓取返回数据经清洗后为空，跳过写库")
            return 0

        records = list(
            df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]
            .itertuples(index=False, name=None)
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO stock_daily "
                "(symbol, date, open, high, low, close, volume, turnover) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                records,
            )
            conn.commit()

        if failed_symbols:
            logger.warning(
                "sync_today_bulk: 写入 %d 条数据，失败 %d 只 ETF，重试 %d 次",
                count,
                len(failed_symbols),
                retry_count,
            )
            logger.warning("失败样例代码: %s", ",".join(sorted(set(failed_symbols))[:30]))
        else:
            logger.info("sync_today_bulk: 写入 %d 条数据（重试 %d 次）", count, retry_count)
        return count

    def backfill(self, symbols: list[str]) -> None:
        """通过 baostock 批量回填历史日 K 线数据（后复权）。

        容错机制：
        - 单只股票失败自动重试 3 次，间隔递增（2s/4s/8s）
        - 每 200 只股票自动重连 baostock（防止长连接超时）
        - 已入库的自动 skip，中断后可重跑续传
        """
        import time
        from datetime import date, timedelta

        import baostock as bs

        today_str = date.today().strftime("%Y-%m-%d")
        max_retries = 3
        reconnect_interval = 200  # 每处理 N 只股票重连一次

        def _login():
            lg = bs.login()
            if lg.error_code != "0":
                logger.error(f"baostock 登录失败: {lg.error_msg}")
                return False
            return True

        if not _login():
            return

        success = 0
        skipped = 0
        failed = 0
        since_reconnect = 0

        try:
            for i, symbol in enumerate(symbols):
                last_date = self._get_last_date(symbol)
                if last_date and last_date >= today_str:
                    skipped += 1
                    if (i + 1) % 500 == 0:
                        logger.info(
                            f"已处理 {i + 1}/{len(symbols)}，"
                            f"成功 {success} 跳过 {skipped} 失败 {failed}"
                        )
                    continue

                # 定期重连，防止长连接超时
                since_reconnect += 1
                if since_reconnect >= reconnect_interval:
                    bs.logout()
                    time.sleep(1)
                    if not _login():
                        logger.error("重连失败，终止回填")
                        return
                    since_reconnect = 0

                start = last_date or self.start_date
                if last_date:
                    start = (date.fromisoformat(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")

                bs_code = self._to_baostock_code(symbol)

                # 带重试的查询
                rows = []
                query_ok = False
                for attempt in range(max_retries):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            "date,open,high,low,close,volume,amount",
                            start_date=start,
                            end_date=today_str,
                            frequency="d",
                            adjustflag="1",  # 后复权
                        )

                        if rs.error_code != "0":
                            raise RuntimeError(rs.error_msg)

                        rows = []
                        while rs.next():
                            rows.append(rs.get_row_data())
                        query_ok = True
                        break

                    except Exception as exc:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            # 重连 baostock
                            bs.logout()
                            time.sleep(1)
                            _login()
                        else:
                            logger.warning(f"[{symbol}] {max_retries}次重试均失败，跳过")

                if not query_ok:
                    failed += 1
                    continue

                if not rows:
                    skipped += 1
                    continue

                df = pd.DataFrame(rows, columns=rs.fields)
                for col in ["open", "high", "low", "close", "volume", "amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df[df["volume"] > 0]

                if df.empty:
                    skipped += 1
                    continue

                df["symbol"] = symbol
                df = df.rename(columns={"amount": "turnover"})
                df = df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]

                try:
                    with sqlite3.connect(self.db_path) as conn:
                        df.to_sql(
                            "stock_daily", conn, if_exists="append",
                            index=False, method="multi", chunksize=500,
                        )
                except sqlite3.IntegrityError:
                    pass

                success += 1

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

        finally:
            bs.logout()

        logger.info(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")

    # ── 场内 ETF 列表 ──

    def get_all_symbols(self) -> list[str]:
        """通过 baostock 获取全市场 A 股场内 ETF 代码列表（type=5，上市 status=1）。"""
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            logger.error(f"baostock 登录失败: {lg.error_msg}")
            return []

        try:
            rs = bs.query_stock_basic(code_name="", code="")
            symbols = []
            while rs.next():
                row = rs.get_row_data()
                code = row[0]  # "sh.510300" or "sz.159919"
                sec_type = row[4]  # type：1 股票，5 ETF（见 baostock 文档）
                listing_status = row[5]  # status：1 上市，0 退市
                if listing_status == "1" and sec_type == "5":
                    symbols.append(code.split(".")[1])  # 纯数字代码
            logger.info(f"获取场内 ETF 列表完成，共 {len(symbols)} 只")
            return symbols
        except Exception as e:
            logger.error(f"获取场内 ETF 列表失败: {e}")
            return []
        finally:
            bs.logout()

    def get_local_symbols(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        return [row[0] for row in rows]

    def get_latest_trade_date(self) -> str | None:
        """全市场 K 线中的最新交易日（MAX(date)）。"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()
        return row[0] if row and row[0] else None

    def get_next_trading_date_after(self, d: str) -> str | None:
        """严格晚于 d 的最早交易日。"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MIN(date) FROM stock_daily WHERE date > ?",
                (d,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def save_digest_top_picks(self, asof_date: str, codes: list[str]) -> None:
        """保存当日综合 Top 代码列表（JSON 数组），同 asof 重复运行则覆盖。"""
        payload = json.dumps(codes, ensure_ascii=False)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO digest_top_picks (asof_date, codes) VALUES (?, ?)",
                (asof_date, payload),
            )
            conn.commit()

    def load_digest_top_picks_strictly_before(self, asof_date: str) -> tuple[str | None, list[str]]:
        """取严格早于 asof_date 的最近一期保存记录。"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT asof_date, codes FROM digest_top_picks WHERE asof_date < ? "
                "ORDER BY asof_date DESC LIMIT 1",
                (asof_date,),
            ).fetchone()
        if not row:
            return None, []
        return row[0], json.loads(row[1])

    def load_digest_top_picks_second_latest(self) -> tuple[str | None, list[str]]:
        """取按 asof_date 倒序的第二新一条（用于与「最新行情日」同一天重复跑时仍能对照上一期）。"""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT asof_date, codes FROM digest_top_picks "
                "ORDER BY asof_date DESC LIMIT 1 OFFSET 1"
            ).fetchone()
        if not row:
            return None, []
        return row[0], json.loads(row[1])
