"""上一期保存的综合推荐在下一交易日的两种收益率统计。"""

from __future__ import annotations

from dataclasses import dataclass

from sequoia_x.data.engine import DataEngine


@dataclass(frozen=True)
class PickDayReturn:
    code: str
    pct_open_buy: float | None  # 下一交易日开盘买、收盘卖
    pct_close_buy: float | None  # 推荐日收盘买，下一交易日收盘卖


def compute_pick_followthrough(
    engine: DataEngine,
    prev_asof: str,
    d_next: str,
    codes: list[str],
    max_rows: int = 10,
) -> tuple[list[PickDayReturn], float | None, float | None]:
    """对 prev_asof 日保存的推荐代码，用 d_next 日 K 线计算两种涨跌幅百分比。

    Returns:
        (rows, avg_open_buy_pct, avg_close_buy_pct)，无有效样本时均值为 None。
    """
    codes = codes[:max_rows]
    rows: list[PickDayReturn] = []
    opens: list[float] = []
    closes: list[float] = []

    for code in codes:
        try:
            df = engine.get_ohlcv(code)
            if df.empty:
                rows.append(PickDayReturn(code, None, None))
                continue
            r_asof = df[df["date"] == prev_asof]
            r_next = df[df["date"] == d_next]
            if r_asof.empty or r_next.empty:
                rows.append(PickDayReturn(code, None, None))
                continue

            o_next = float(r_next.iloc[0]["open"])
            c_next = float(r_next.iloc[0]["close"])
            c_asof = float(r_asof.iloc[0]["close"])

            pct_o: float | None = None
            if o_next > 0:
                pct_o = (c_next / o_next - 1.0) * 100.0
                opens.append(pct_o)

            pct_c: float | None = None
            if c_asof > 0:
                pct_c = (c_next / c_asof - 1.0) * 100.0
                closes.append(pct_c)

            rows.append(PickDayReturn(code, pct_o, pct_c))
        except Exception:
            rows.append(PickDayReturn(code, None, None))

    avg_o = sum(opens) / len(opens) if opens else None
    avg_c = sum(closes) / len(closes) if closes else None
    return rows, avg_o, avg_c


def format_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}%"
