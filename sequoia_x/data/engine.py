"""数据引擎模块：负责 PostgreSQL 行情数据存储与 baostock 增量同步。"""

import json

import pandas as pd
import psycopg

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


_CREATE_TABLE_SQL = """
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

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""

_CREATE_DIGEST_PICKS_SQL = """
CREATE TABLE IF NOT EXISTS digest_top_picks (
    asof_date TEXT PRIMARY KEY,
    codes     TEXT NOT NULL
);
"""

_UPSERT_STOCK_DAILY_SQL = """
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

_UPSERT_DIGEST_SQL = """
INSERT INTO digest_top_picks (asof_date, codes)
VALUES (%s, %s)
ON CONFLICT (asof_date) DO UPDATE SET
    codes = EXCLUDED.codes
"""

_OHLCV_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]


def parse_digest_top_picks_payload(raw: str | list | dict) -> tuple[list[str], dict[str, tuple[int, float]]]:
    """解析 digest_top_picks.codes 列：兼容旧版 JSON 数组与新版 {codes, rows}。"""
    if isinstance(raw, str):
        data = json.loads(raw)
    else:
        data = raw
    if isinstance(data, list):
        return [str(c) for c in data], {}
    if not isinstance(data, dict):
        return [], {}
    codes = data.get("codes") or []
    codes = [str(c) for c in codes]
    stats: dict[str, tuple[int, float]] = {}
    for row in data.get("rows") or []:
        if not isinstance(row, dict):
            continue
        c = row.get("code")
        if not c:
            continue
        stats[str(c)] = (int(row.get("vote_count", 0)), float(row.get("vote_score", 0.0)))
    return codes, stats

_OHLCV_COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]


def _akshare_etf_daily_range(symbol: str, range_start: str, range_end: str) -> pd.DataFrame:
    """东方财富 ETF 日 K（后复权 hfq），与 baostock adjustflag=1 口径一致为后复权。

    在部分网络环境下 baostock 历史区间会返回空行，用作区间回填的兜底数据源。
    """
    import time

    import akshare as ak

    beg = range_start.replace("-", "")
    end = range_end.replace("-", "")
    max_retries = 3
    raw: pd.DataFrame | None = None
    for attempt in range(max_retries):
        try:
            raw = ak.fund_etf_hist_em(
                symbol=symbol,
                period="daily",
                start_date=beg,
                end_date=end,
                adjust="hfq",
            )
            break
        except Exception as exc:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "[%s] akshare 区间第 %d 次失败: %s，%ds 后重试",
                    symbol,
                    attempt + 1,
                    exc,
                    wait,
                )
                time.sleep(wait)
            else:
                logger.warning("[%s] akshare 区间 %d 次重试均失败", symbol, max_retries)
                return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.to_datetime(raw["日期"], errors="coerce").dt.strftime("%Y-%m-%d"),
            "open": pd.to_numeric(raw["开盘"], errors="coerce"),
            "high": pd.to_numeric(raw["最高"], errors="coerce"),
            "low": pd.to_numeric(raw["最低"], errors="coerce"),
            "close": pd.to_numeric(raw["收盘"], errors="coerce"),
            "volume": pd.to_numeric(raw["成交量"], errors="coerce"),
            "turnover": pd.to_numeric(raw["成交额"], errors="coerce"),
        }
    )
    out = out.dropna(subset=["close", "date"])
    out = out[out["volume"] > 0]
    return out


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
    """行情数据引擎，负责 PostgreSQL 存储和 baostock 数据同步。"""

    def __init__(self, settings: Settings) -> None:
        self.database_url: str = settings.database_url
        self.start_date: str = settings.start_date
        # 运行期只读缓存：回测阶段会被多个策略重复访问同一批 symbol。
        # 通过缓存降低重复 SQL 查询开销，写库后统一失效。
        self._local_symbols_cache: list[str] | None = None
        self._ohlcv_cache: dict[str, pd.DataFrame] = {}
        self._init_db()

    def _invalidate_runtime_cache(self) -> None:
        self._local_symbols_cache = None
        self._ohlcv_cache.clear()

    def _connect(self):
        return psycopg.connect(self.database_url)

    @staticmethod
    def _rows_to_ohlcv_df(rows: list[tuple]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=_OHLCV_COLUMNS)
        return pd.DataFrame(rows, columns=_OHLCV_COLUMNS)

    @staticmethod
    def _rows_to_ohlcv_df(rows: list[tuple]) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=_OHLCV_COLUMNS)
        return pd.DataFrame(rows, columns=_OHLCV_COLUMNS)

    def _init_db(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_TABLE_SQL)
                cur.execute(_CREATE_INDEX_SQL)
                cur.execute(_CREATE_DIGEST_PICKS_SQL)
            conn.commit()
        logger.info("PostgreSQL 数据库初始化完成")

    def _get_last_date(self, symbol: str) -> str | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(date) FROM stock_daily WHERE symbol = %s", (symbol,))
                row = cur.fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        cached = self._ohlcv_cache.get(symbol)
        if cached is not None:
            # 返回副本，避免策略内新增/覆盖列污染缓存。
            return cached.copy()

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol, date, open, high, low, close, volume, turnover "
                    "FROM stock_daily WHERE symbol = %s ORDER BY date",
                    (symbol,),
                )
                rows = cur.fetchall()
        df = self._rows_to_ohlcv_df(rows)
        self._ohlcv_cache[symbol] = df
        return df.copy()

    def preload_ohlcv_cache(self, recent_bars: int | None = None) -> int:
        """批量预热行情缓存，减少策略批跑时的逐 symbol SQL 往返。

        Args:
            recent_bars: 仅缓存每个 symbol 最近 N 根 K 线；None 表示全量缓存。

        Returns:
            已缓存的 symbol 数量。
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                if recent_bars is None:
                    cur.execute(
                        "SELECT symbol, date, open, high, low, close, volume, turnover "
                        "FROM stock_daily ORDER BY symbol, date"
                    )
                    rows = cur.fetchall()
                else:
                    cur.execute(
                        """
                        SELECT symbol, date, open, high, low, close, volume, turnover
                        FROM (
                            SELECT
                                symbol, date, open, high, low, close, volume, turnover,
                                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
                            FROM stock_daily
                        ) ranked
                        WHERE rn <= %s
                        ORDER BY symbol, date
                        """,
                        (recent_bars,),
                    )
                    rows = cur.fetchall()

        symbol_rows: dict[str, list[tuple]] = {}
        for row in rows:
            symbol = str(row[0])
            symbol_rows.setdefault(symbol, []).append(row)

        self._ohlcv_cache = {
            symbol: self._rows_to_ohlcv_df(items)
            for symbol, items in symbol_rows.items()
        }
        self._local_symbols_cache = sorted(self._ohlcv_cache.keys())

        logger.info(
            "行情缓存预热完成：symbols=%d, mode=%s",
            len(self._local_symbols_cache),
            "full" if recent_bars is None else f"recent_{recent_bars}",
        )
        return len(self._local_symbols_cache)

    def preload_ohlcv_cache_date_range(self, min_date: str, max_date: str) -> int:
        """按交易日区间批量加载日 K，用于回测等场景，避免全表读入内存。"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, date, open, high, low, close, volume, turnover
                    FROM stock_daily
                    WHERE date >= %s AND date <= %s
                    ORDER BY symbol, date
                    """,
                    (min_date, max_date),
                )
                rows = cur.fetchall()

        symbol_rows: dict[str, list[tuple]] = {}
        for row in rows:
            symbol = str(row[0])
            symbol_rows.setdefault(symbol, []).append(row)

        self._ohlcv_cache = {
            symbol: self._rows_to_ohlcv_df(items) for symbol, items in symbol_rows.items()
        }
        self._local_symbols_cache = sorted(self._ohlcv_cache.keys())

        logger.info(
            "行情缓存按区间预热完成：symbols=%d, date_range=[%s, %s], rows=%d",
            len(self._local_symbols_cache),
            min_date,
            max_date,
            len(rows),
        )
        return len(self._local_symbols_cache)

    @staticmethod
    def _to_baostock_code(symbol: str) -> str:
        """将纯数字代码转为 baostock 格式：沪（5/6/9）-> sh，其余 -> sz（含深市 ETF）。"""
        prefix = "sh" if symbol.startswith(("5", "6", "9")) else "sz"
        return f"{prefix}.{symbol}"

    # ── 数据同步 ──

    def sync_today_bulk(self) -> int:
        """多进程并行通过 baostock 拉取增量数据（后复权），幂等写入 PostgreSQL。"""
        from datetime import date, timedelta
        from multiprocessing import Pool

        today_str = date.today().strftime("%Y-%m-%d")

        tasks = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, MAX(date) FROM stock_daily GROUP BY symbol")
                rows = cur.fetchall()

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
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(_UPSERT_STOCK_DAILY_SQL, records)
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
        self._invalidate_runtime_cache()
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

                records = list(
                    df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]
                    .itertuples(index=False, name=None)
                )
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.executemany(_UPSERT_STOCK_DAILY_SQL, records)
                    conn.commit()

                success += 1

                if (i + 1) % 500 == 0:
                    logger.info(
                        f"已处理 {i + 1}/{len(symbols)}，"
                        f"成功 {success} 跳过 {skipped} 失败 {failed}"
                    )

        finally:
            bs.logout()

        self._invalidate_runtime_cache()
        logger.info(f"回填完成 — 成功: {success} | 跳过: {skipped} | 失败: {failed}")

    def backfill_date_range(self, symbols: list[str], range_start: str, range_end: str) -> None:
        """按固定日历区间拉取日 K 并 upsert（后复权）。

        优先 baostock；若该标的在区间内无返回或清洗后为空，则回退 akshare
        ``fund_etf_hist_em``（东方财富，hfq），以便在 baostock 历史接口异常时仍能灌库。

        与 ``backfill`` 不同：不依赖各标的 ``MAX(date)``，用于补齐早于当前库中
        最早记录的历史窗口（例如将全体 ETF 的 2023 年灌库）。

        未上市或无成交的区间返回空，记为跳过。
        """
        import time

        import baostock as bs

        max_retries = 3
        reconnect_interval = 200

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
                since_reconnect += 1
                if since_reconnect >= reconnect_interval:
                    bs.logout()
                    time.sleep(1)
                    if not _login():
                        logger.error("重连失败，终止区间回填")
                        return
                    since_reconnect = 0

                bs_code = self._to_baostock_code(symbol)
                rows = []
                query_ok = False
                rs_fields: list[str] = []
                for attempt in range(max_retries):
                    try:
                        rs = bs.query_history_k_data_plus(
                            bs_code,
                            "date,open,high,low,close,volume,amount",
                            start_date=range_start,
                            end_date=range_end,
                            frequency="d",
                            adjustflag="1",
                        )
                        if rs.error_code != "0":
                            raise RuntimeError(rs.error_msg)
                        rows = []
                        while rs.next():
                            rows.append(rs.get_row_data())
                        rs_fields = list(rs.fields)
                        query_ok = True
                        break
                    except Exception as exc:
                        if attempt < max_retries - 1:
                            wait = 2 ** (attempt + 1)
                            logger.warning(
                                f"[{symbol}] 区间回填第{attempt + 1}次失败: {exc}，{wait}s 后重试"
                            )
                            time.sleep(wait)
                            bs.logout()
                            time.sleep(1)
                            _login()
                        else:
                            logger.warning(f"[{symbol}] 区间回填 {max_retries} 次重试均失败，跳过")

                if not query_ok:
                    failed += 1
                    continue

                df = pd.DataFrame()
                if rows:
                    df = pd.DataFrame(rows, columns=rs_fields)
                    for col in ["open", "high", "low", "close", "volume", "amount"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna(subset=["close"])
                    df = df[df["volume"] > 0]
                    if not df.empty:
                        df["symbol"] = symbol
                        df = df.rename(columns={"amount": "turnover"})
                        df = df[
                            ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
                        ]

                if df.empty:
                    df = _akshare_etf_daily_range(symbol, range_start, range_end)
                    if not df.empty:
                        time.sleep(0.12)

                if df.empty:
                    skipped += 1
                    continue

                records = list(
                    df[["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]]
                    .itertuples(index=False, name=None)
                )
                with self._connect() as conn:
                    with conn.cursor() as cur:
                        cur.executemany(_UPSERT_STOCK_DAILY_SQL, records)
                    conn.commit()

                success += 1
                if (i + 1) % 500 == 0:
                    logger.info(
                        f"区间回填已处理 {i + 1}/{len(symbols)} "
                        f"[{range_start}~{range_end}] 成功 {success} 跳过 {skipped} 失败 {failed}"
                    )
        finally:
            bs.logout()

        self._invalidate_runtime_cache()
        logger.info(
            f"区间回填完成 [{range_start}~{range_end}] — 成功: {success} | 跳过: {skipped} | 失败: {failed}"
        )

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
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT symbol FROM stock_daily ORDER BY symbol")
                rows = cur.fetchall()
        return [row[0] for row in rows]

    def get_latest_trade_date(self) -> str | None:
        """全市场 K 线中的最新交易日（MAX(date)）。"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(date) FROM stock_daily")
                row = cur.fetchone()
        return row[0] if row and row[0] else None

    def get_next_trading_date_after(self, d: str) -> str | None:
        """严格晚于 d 的最早交易日。"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MIN(date) FROM stock_daily WHERE date > %s", (d,))
                row = cur.fetchone()
        return row[0] if row and row[0] else None

    def save_digest_top_picks(
        self,
        asof_date: str,
        codes: list[str],
        *,
        pick_rows: list[dict[str, float | int | str]] | None = None,
    ) -> None:
        """保存当日综合 Top：新版为 JSON 对象含 codes 与 rows（票/分），旧调用方仅传 codes 时仍写数组。"""
        if pick_rows is not None:
            payload_obj: list | dict = {
                "codes": codes,
                "rows": pick_rows,
            }
        else:
            payload_obj = codes
        payload = json.dumps(payload_obj, ensure_ascii=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_UPSERT_DIGEST_SQL, (asof_date, payload))
            conn.commit()

    def load_digest_top_picks_strictly_before(
        self, asof_date: str
    ) -> tuple[str | None, list[str], dict[str, tuple[int, float]]]:
        """取严格早于 asof_date 的最近一期保存记录。"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT asof_date, codes FROM digest_top_picks WHERE asof_date < %s "
                    "ORDER BY asof_date DESC LIMIT 1",
                    (asof_date,),
                )
                row = cur.fetchone()
        if not row:
            return None, [], {}
        codes, stats = parse_digest_top_picks_payload(row[1])
        return row[0], codes, stats

    def load_digest_top_picks_second_latest(
        self,
    ) -> tuple[str | None, list[str], dict[str, tuple[int, float]]]:
        """取按 asof_date 倒序的第二新一条（用于与「最新行情日」同一天重复跑时仍能对照上一期）。"""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT asof_date, codes FROM digest_top_picks "
                    "ORDER BY asof_date DESC LIMIT 1 OFFSET 1"
                )
                row = cur.fetchone()
        if not row:
            return None, [], {}
        codes, stats = parse_digest_top_picks_payload(row[1])
        return row[0], codes, stats
