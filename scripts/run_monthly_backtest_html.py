#!/usr/bin/env python3
"""运行月度回测并生成 HTML 详细交易报告。"""

from __future__ import annotations

import argparse
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

from main import build_aggressive_settings, estimate_recent_bars_for_profiles, run_profile_strategies
from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.digest import TopPick, rank_top_picks
from sequoia_x.strategy.market_regime_filter import MarketRegimeFilter

logger = get_logger(__name__)


@dataclass
class TradeRecord:
    profile: str
    signal_date: str
    buy_date: str
    sell_date: str
    buy_time: str
    sell_time: str
    symbol: str
    capital_before: float
    buy_open: float | None
    sell_open: float | None
    buy_fee: float
    sell_fee: float
    capital_after: float
    pnl: float
    ret_pct: float
    status: str
    reason: str


class SnapshotEngine:
    """将 DataEngine 的全量缓存裁切到某个信号日，供策略复用。"""

    def __init__(self, all_data: dict[str, object], asof_date: str) -> None:
        self._all_data = all_data
        self._asof_date = asof_date
        self._symbols = sorted(all_data.keys())
        self._slice_end: dict[str, int] = {}
        self._date_np: dict[str, np.ndarray] = {}
        for symbol in self._symbols:
            src = all_data.get(symbol)
            if src is not None and len(src) > 0:  # type: ignore[arg-type]
                arr = src["date"].astype(str).to_numpy()  # type: ignore[index]
                self._date_np[symbol] = arr
                self._slice_end[symbol] = int(np.searchsorted(arr, self._asof_date, side="right"))
            else:
                self._date_np[symbol] = np.array([], dtype=object)
                self._slice_end[symbol] = 0

    def get_local_symbols(self) -> list[str]:
        return list(self._symbols)

    def get_ohlcv(self, symbol: str):
        import pandas as pd

        src = self._all_data.get(symbol)
        if src is None or getattr(src, "empty", True) or len(src) == 0:  # type: ignore[arg-type]
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume", "turnover"])

        right = self._slice_end[symbol]
        # 浅拷贝：策略会新增列；与全量缓存隔离，避免重复深拷贝整段历史。
        return src.iloc[:right].copy(deep=False)  # type: ignore[union-attr]


def _month_range(month_str: str) -> tuple[str, str]:
    month_start = date.fromisoformat(f"{month_str}-01")
    if month_start.month == 12:
        next_month = date(month_start.year + 1, 1, 1)
    else:
        next_month = date(month_start.year, month_start.month + 1, 1)
    month_end = next_month - timedelta(days=1)
    return month_start.isoformat(), month_end.isoformat()


def _backtest_report_filename(period_start: str, period_end: str, run_tag: str) -> str:
    """月度 HTML 文件名：历史回测-开始日期_截止日期-报告生成时间.html"""
    return f"历史回测-{period_start}_{period_end}-{run_tag}.html"


def _cn_ret_color_class(ret: float) -> str:
    """A 股习惯：涨为红，跌为绿。"""
    if ret > 0:
        return "ret-rise"
    if ret < 0:
        return "ret-fall"
    return "ret-flat"


def _fetch_trade_dates(engine: DataEngine, start_date: str, end_date: str) -> list[str]:
    with engine._connect() as conn:  # noqa: SLF001
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT date FROM stock_daily WHERE date BETWEEN %s AND %s ORDER BY date",
                (start_date, end_date),
            )
            rows = cur.fetchall()
    return [str(r[0]) for r in rows]


def _build_turnover_by_symbol(snapshot_engine: SnapshotEngine, symbols: set[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for sym in symbols:
        try:
            df = snapshot_engine.get_ohlcv(sym)
            if df.empty:
                continue
            out[sym] = float(df.iloc[-1]["turnover"])
        except Exception:
            continue
    return out


def _pick_top1(
    strategy_hits: dict[str, list[str]],
    turnover: dict[str, float],
    weights: dict[str, float],
    groups: dict[str, str],
    group_multipliers: dict[str, float],
) -> TopPick | None:
    picks = rank_top_picks(
        strategy_hits,
        turnover,
        top_n=5,
        strategy_weights=weights,
        strategy_groups=groups,
        group_multipliers=group_multipliers,
    )
    return picks[0] if picks else None


def _default_signal_day_workers(n_signal_days: int) -> int:
    """按信号日并行时的线程数。

    默认 ``1``（仅日内双画像并行）：全市场 pandas 在 CPython 上按日并行常因 GIL /
    内存带宽反而变慢。若机器核多且实测有收益，可设环境变量 ``BACKTEST_DAY_WORKERS`` 或
    命令行 ``--day-workers``。
    """
    raw = os.environ.get("BACKTEST_DAY_WORKERS")
    if raw is not None and str(raw).strip() != "":
        try:
            w = int(str(raw).strip())
            return max(1, min(w, n_signal_days))
        except ValueError:
            pass
    return 1


def _compute_signal_day_picks(
    day_index: int,
    signal_date: str,
    buy_date: str,
    sell_date: str,
    all_data: dict[str, object],
    settings: object,
    aggressive_settings: object,
    inner_profile_parallel: bool,
) -> dict[str, object]:
    """单日：双画像跑策略并产出 Top1；各日之间无依赖时可由线程池并行调度。"""
    snapshot = SnapshotEngine(all_data, signal_date)
    regime = MarketRegimeFilter(snapshot, settings).detect()  # type: ignore[arg-type]
    if inner_profile_parallel:
        with ThreadPoolExecutor(max_workers=2) as executor:
            conservative_future = executor.submit(
                run_profile_strategies,
                snapshot,
                settings,
                regime,
                logger,
                "稳健版",
                quiet=True,
            )
            aggressive_future = executor.submit(
                run_profile_strategies,
                snapshot,
                aggressive_settings,
                regime,
                logger,
                "激进版",
                quiet=True,
            )
            conservative_hits = conservative_future.result()
            aggressive_hits = aggressive_future.result()
    else:
        conservative_hits = run_profile_strategies(
            snapshot,
            settings,
            regime,
            logger,
            "稳健版",
            quiet=True,
        )
        aggressive_hits = run_profile_strategies(
            snapshot,
            aggressive_settings,
            regime,
            logger,
            "激进版",
            quiet=True,
        )

    hits_map = {"稳健版": conservative_hits, "激进版": aggressive_hits}
    settings_map = {"稳健版": settings, "激进版": aggressive_settings}
    pick_by_profile: dict[str, TopPick | None] = {}
    for profile in ("稳健版", "激进版"):
        strategy_hits = hits_map[profile]
        profile_settings = settings_map[profile]
        hit_symbols: set[str] = set()
        for values in strategy_hits.values():
            hit_symbols.update(values)
        turnover = _build_turnover_by_symbol(snapshot, hit_symbols)
        pick_by_profile[profile] = _pick_top1(
            strategy_hits=strategy_hits,
            turnover=turnover,
            weights=profile_settings.get_strategy_weights(),
            groups=profile_settings.get_strategy_groups(),
            group_multipliers=profile_settings.get_regime_group_multipliers(regime.regime),
        )

    return {
        "day_index": day_index,
        "signal_date": signal_date,
        "buy_date": buy_date,
        "sell_date": sell_date,
        "pick_conservative": pick_by_profile["稳健版"],
        "pick_aggressive": pick_by_profile["激进版"],
    }


def _html_escape(value: object) -> str:
    text = str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _fmt_money(v: float) -> str:
    return f"{v:,.2f}"


def _fmt_pct(v: float) -> str:
    return f"{v * 100:+.4f}%"


def _calc_max_drawdown(equity: list[float]) -> float:
    peak = 0.0
    mdd = 0.0
    for v in equity:
        peak = max(peak, v)
        if peak <= 0:
            continue
        dd = v / peak - 1.0
        if dd < mdd:
            mdd = dd
    return mdd


def build_html(
    *,
    title: str,
    month: str,
    period_start: str,
    period_end: str,
    run_started_at: str,
    run_finished_at: str,
    elapsed_sec: float,
    warmup_sec: float,
    simulate_sec: float,
    signal_count: int,
    fee_rate: float,
    records: list[TradeRecord],
    capitals: dict[str, float],
    initial_capital: float,
) -> str:
    profile_records: dict[str, list[TradeRecord]] = {"稳健版": [], "激进版": []}
    for r in records:
        profile_records.setdefault(r.profile, []).append(r)

    summary_rows: list[str] = []
    for profile, items in profile_records.items():
        executed = [x for x in items if x.status == "EXECUTED"]
        wins = sum(1 for x in executed if x.ret_pct > 0)
        win_rate = (wins / len(executed)) if executed else 0.0
        avg_ret = sum(x.ret_pct for x in executed) / len(executed) if executed else 0.0
        equity = [initial_capital] + [x.capital_after for x in items]
        mdd = _calc_max_drawdown(equity)
        final_cap = capitals.get(profile, initial_capital)
        cum_ret = final_cap / initial_capital - 1.0
        cum_cls = _cn_ret_color_class(cum_ret)
        avg_cls = _cn_ret_color_class(avg_ret)
        mdd_cls = _cn_ret_color_class(mdd)
        summary_rows.append(
            "<tr>"
            f"<td>{_html_escape(profile)}</td>"
            f"<td class='num'>{_fmt_money(final_cap)}</td>"
            f"<td class='num {cum_cls}'>{_fmt_pct(cum_ret)}</td>"
            f"<td class='num'>{len(executed)}</td>"
            f"<td class='num'>{wins}</td>"
            f"<td class='num'>{_fmt_pct(win_rate)}</td>"
            f"<td class='num {avg_cls}'>{_fmt_pct(avg_ret)}</td>"
            f"<td class='num {mdd_cls}'>{_fmt_pct(mdd)}</td>"
            "</tr>"
        )

    detail_rows: list[str] = []
    for idx, r in enumerate(records, start=1):
        detail_rows.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{_html_escape(r.profile)}</td>"
            f"<td>{_html_escape(r.signal_date)}</td>"
            f"<td>{_html_escape(r.buy_time)}</td>"
            f"<td>{_html_escape(r.sell_time)}</td>"
            f"<td>{_html_escape(r.symbol)}</td>"
            f"<td class='num'>{'-' if r.buy_open is None else f'{r.buy_open:.4f}'}</td>"
            f"<td class='num'>{'-' if r.sell_open is None else f'{r.sell_open:.4f}'}</td>"
            f"<td class='num'>{_fmt_money(r.capital_before)}</td>"
            f"<td class='num'>{_fmt_money(r.buy_fee)}</td>"
            f"<td class='num'>{_fmt_money(r.sell_fee)}</td>"
            f"<td class='num'>{_fmt_money(r.capital_after)}</td>"
            f"<td class='num'>{_fmt_money(r.pnl)}</td>"
            f"<td class='num'>{_fmt_pct(r.ret_pct)}</td>"
            f"<td>{_html_escape(r.status)}</td>"
            f"<td>{_html_escape(r.reason)}</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{_html_escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; margin: 24px; color: #111; }}
    h1, h2 {{ margin: 8px 0 12px; }}
    .meta {{ margin-bottom: 20px; }}
    .meta span {{ display: inline-block; margin-right: 18px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 24px; font-size: 13px; }}
    th, td {{ border: 1px solid #d9d9d9; padding: 6px 8px; }}
    th {{ background: #f5f5f5; text-align: left; position: sticky; top: 0; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .wrap {{ max-height: 72vh; overflow: auto; border: 1px solid #e6e6e6; }}
  </style>
</head>
<body>
  <h1>{_html_escape(title)}</h1>
  <div class="meta">
    <span><b>回测月份：</b>{_html_escape(month)}</span>
    <span><b>回测时间段：</b>{_html_escape(period_start)} 至 {_html_escape(period_end)}</span>
    <span><b>开始时间：</b>{_html_escape(run_started_at)}</span>
    <span><b>结束时间：</b>{_html_escape(run_finished_at)}</span>
    <span><b>总耗时：</b>{elapsed_sec:.3f}s</span>
    <span><b>缓存预热：</b>{warmup_sec:.3f}s</span>
    <span><b>回测模拟：</b>{simulate_sec:.3f}s</span>
    <span><b>信号日数量：</b>{signal_count}</span>
    <span><b>手续费（双边）：</b>{fee_rate * 100:.4f}% × 2</span>
  </div>

  <h2>画像汇总</h2>
  <table>
    <thead>
      <tr>
        <th>画像</th><th class="num">期末资金</th><th class="num">累计收益</th><th class="num">成交次数</th>
        <th class="num">盈利笔数</th><th class="num">胜率</th><th class="num">单笔平均收益</th><th class="num">最大回撤</th>
      </tr>
    </thead>
    <tbody>
      {"".join(summary_rows)}
    </tbody>
  </table>

  <h2>交易明细（按时间顺序）</h2>
  <div class="wrap">
    <table>
      <thead>
        <tr>
          <th>#</th><th>画像</th><th>信号日</th><th>买入时间</th><th>卖出时间</th><th>标的</th>
          <th class="num">买入开盘价</th><th class="num">卖出开盘价</th><th class="num">交易金额(期初资金)</th>
          <th class="num">买入手续费</th><th class="num">卖出手续费</th><th class="num">期末资金</th>
          <th class="num">本笔盈亏</th><th class="num">本笔收益率</th><th>状态</th><th>备注</th>
        </tr>
      </thead>
      <tbody>
        {"".join(detail_rows)}
      </tbody>
    </table>
  </div>
</body>
</html>
"""


def run_backtest_for_month(
    month: str,
    fee_rate: float,
    initial_capital: float,
    *,
    day_workers: int | None = None,
) -> tuple[Path, float]:
    started_perf = time.perf_counter()
    started_at = datetime.now()

    settings = get_settings()
    engine = DataEngine(settings)
    recent_bars = estimate_recent_bars_for_profiles(settings)

    month_start, month_end = _month_range(month)
    all_dates = _fetch_trade_dates(engine, month_start, month_end)
    if len(all_dates) < 3:
        raise RuntimeError(f"{month} 交易日不足，无法进行 O->O 隔夜回测。")

    lookback_days = max(450, int(recent_bars * 1.55))
    data_start = (
        date.fromisoformat(all_dates[0]) - timedelta(days=lookback_days)
    ).isoformat()
    data_end = all_dates[-1]

    warmup_start = time.perf_counter()
    engine.preload_ohlcv_cache_date_range(data_start, data_end)
    warmup_elapsed = time.perf_counter() - warmup_start

    all_data = dict(engine._ohlcv_cache)  # noqa: SLF001
    price_lookup: dict[str, dict[str, float]] = {}
    for symbol, df in all_data.items():
        try:
            sub = df[["date", "open"]].dropna(subset=["open"])
            price_lookup[symbol] = {str(row["date"]): float(row["open"]) for _, row in sub.iterrows()}
        except Exception:
            continue

    profiles = ("稳健版", "激进版")
    capitals: dict[str, float] = {p: initial_capital for p in profiles}
    records: list[TradeRecord] = []
    aggressive_settings = build_aggressive_settings(settings)

    simulate_start = time.perf_counter()
    for i in range(len(all_dates) - 2):
        signal_date = all_dates[i]
        buy_date = all_dates[i + 1]
        sell_date = all_dates[i + 2]

        snapshot = SnapshotEngine(all_data, signal_date)
        regime = MarketRegimeFilter(snapshot, settings).detect()

        with ThreadPoolExecutor(max_workers=2) as executor:
            conservative_future = executor.submit(
                run_profile_strategies,
                snapshot,
                settings,
                regime,
                logger,
                "稳健版",
                quiet=True,
            )
            aggressive_future = executor.submit(
                run_profile_strategies,
                snapshot,
                aggressive_settings,
                regime,
                logger,
                "激进版",
                quiet=True,
            )
            conservative_hits = conservative_future.result()
            aggressive_hits = aggressive_future.result()

        hits_map = {"稳健版": conservative_hits, "激进版": aggressive_hits}
        settings_map = {"稳健版": settings, "激进版": aggressive_settings}

        for profile in profiles:
            strategy_hits = hits_map[profile]
            profile_settings = settings_map[profile]
            hit_symbols: set[str] = set()
            for values in strategy_hits.values():
                hit_symbols.update(values)
            turnover = _build_turnover_by_symbol(snapshot, hit_symbols)

            pick = _pick_top1(
                strategy_hits=strategy_hits,
                turnover=turnover,
                weights=profile_settings.get_strategy_weights(),
                groups=profile_settings.get_strategy_groups(),
                group_multipliers=profile_settings.get_regime_group_multipliers(regime.regime),
            )

            cap_before = capitals[profile]
            if pick is None:
                records.append(
                    TradeRecord(
                        profile=profile,
                        signal_date=signal_date,
                        buy_date=buy_date,
                        sell_date=sell_date,
                        buy_time=f"{buy_date} 09:30",
                        sell_time=f"{sell_date} 09:30",
                        symbol="-",
                        capital_before=cap_before,
                        buy_open=None,
                        sell_open=None,
                        buy_fee=0.0,
                        sell_fee=0.0,
                        capital_after=cap_before,
                        pnl=0.0,
                        ret_pct=0.0,
                        status="SKIPPED_NO_SIGNAL",
                        reason="当日无有效Top1信号",
                    )
                )
                continue

            symbol = pick.code
            buy_open = price_lookup.get(symbol, {}).get(buy_date)
            sell_open = price_lookup.get(symbol, {}).get(sell_date)
            if buy_open is None or sell_open is None or buy_open <= 0 or sell_open <= 0:
                records.append(
                    TradeRecord(
                        profile=profile,
                        signal_date=signal_date,
                        buy_date=buy_date,
                        sell_date=sell_date,
                        buy_time=f"{buy_date} 09:30",
                        sell_time=f"{sell_date} 09:30",
                        symbol=symbol,
                        capital_before=cap_before,
                        buy_open=buy_open,
                        sell_open=sell_open,
                        buy_fee=0.0,
                        sell_fee=0.0,
                        capital_after=cap_before,
                        pnl=0.0,
                        ret_pct=0.0,
                        status="SKIPPED_MISSING_OPEN",
                        reason="买入或卖出日开盘价缺失，按现金持有处理",
                    )
                )
                continue

            gross_after = cap_before * (sell_open / buy_open)
            buy_fee = cap_before * fee_rate
            sell_fee = gross_after * fee_rate
            cap_after = gross_after - buy_fee - sell_fee
            pnl = cap_after - cap_before
            ret_pct = cap_after / cap_before - 1.0
            capitals[profile] = cap_after

            records.append(
                TradeRecord(
                    profile=profile,
                    signal_date=signal_date,
                    buy_date=buy_date,
                    sell_date=sell_date,
                    buy_time=f"{buy_date} 09:30",
                    sell_time=f"{sell_date} 09:30",
                    symbol=symbol,
                    capital_before=cap_before,
                    buy_open=buy_open,
                    sell_open=sell_open,
                    buy_fee=buy_fee,
                    sell_fee=sell_fee,
                    capital_after=cap_after,
                    pnl=pnl,
                    ret_pct=ret_pct,
                    status="EXECUTED",
                    reason="Top1 O->O 隔夜交易",
                )
            )

    simulate_elapsed = time.perf_counter() - simulate_start
    finished_at = datetime.now()
    total_elapsed = time.perf_counter() - started_perf

    run_tag = finished_at.strftime("%Y%m%d_%H%M%S")
    title = f"Sequoia-X 月度回测报告 | {month} | 生成于 {finished_at.strftime('%Y-%m-%d %H:%M:%S')}"
    html = build_html(
        title=title,
        month=month,
        period_start=month_start,
        period_end=month_end,
        run_started_at=started_at.strftime("%Y-%m-%d %H:%M:%S"),
        run_finished_at=finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        elapsed_sec=total_elapsed,
        warmup_sec=warmup_elapsed,
        simulate_sec=simulate_elapsed,
        signal_count=max(0, len(all_dates) - 2),
        fee_rate=fee_rate,
        records=records,
        capitals=capitals,
        initial_capital=initial_capital,
    )

    out_dir = Path("results") / "html"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / _backtest_report_filename(month_start, month_end, run_tag)
    out_path.write_text(html, encoding="utf-8")

    logger.info("月度回测完成: month=%s, elapsed=%.3fs, report=%s", month, total_elapsed, out_path)
    print(f"MONTH={month}")
    print(f"STARTED_AT={started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"FINISHED_AT={finished_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"ELAPSED_SECONDS={total_elapsed:.6f}")
    print(f"WARMUP_SECONDS={warmup_elapsed:.6f}")
    print(f"SIMULATION_SECONDS={simulate_elapsed:.6f}")
    print(f"REPORT_PATH={out_path}")
    return out_path, total_elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="运行月度回测并输出 HTML 明细报告")
    parser.add_argument("--month", default=datetime.now().strftime("%Y-%m"), help="回测月份，格式 YYYY-MM")
    parser.add_argument("--fee-rate", type=float, default=0.00005, help="单边手续费率，默认万0.5")
    parser.add_argument("--initial-capital", type=float, default=100000.0, help="初始资金")
    parser.add_argument(
        "--day-workers",
        type=int,
        default=None,
        help="按信号日并行的线程数；默认 1（仅日内双画像并行）。大于 1 时日内双画像改为串行以降低争用；可用环境变量 BACKTEST_DAY_WORKERS 覆盖",
    )
    args = parser.parse_args()

    if len(args.month) != 7 or args.month[4] != "-":
        raise ValueError("--month 必须是 YYYY-MM 格式")
    if args.fee_rate < 0:
        raise ValueError("--fee-rate 不能为负")
    if not math.isfinite(args.initial_capital) or args.initial_capital <= 0:
        raise ValueError("--initial-capital 必须为正数")

    run_backtest_for_month(
        month=args.month,
        fee_rate=args.fee_rate,
        initial_capital=args.initial_capital,
        day_workers=args.day_workers,
    )


if __name__ == "__main__":
    main()