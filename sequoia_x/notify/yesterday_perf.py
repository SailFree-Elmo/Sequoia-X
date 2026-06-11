"""上一期保存的综合推荐在「开盘买 O→O」口径下的收益率统计。

与回测一致：信号日 prev_asof 的次日 d_next 开盘买入，再下一交易日 d_next2 开盘卖出。
"""

from __future__ import annotations

from dataclasses import dataclass

from sequoia_x.data.engine import DataEngine


@dataclass(frozen=True)
class PickDayReturn:
    code: str
    pct_open_buy: float | None  # (d_next2 开盘 / d_next 开盘 - 1) * 100


def compute_pick_followthrough(
    engine: DataEngine,
    d_next: str,
    d_next2: str,
    codes: list[str],
    max_rows: int = 10,
) -> tuple[list[PickDayReturn], float | None]:
    """对保存的推荐代码，用 d_next 与 d_next2 两根日 K 计算开盘买 O→O 涨跌幅。

    Returns:
        (rows, avg_open_buy_pct)，无有效样本时均值为 None。
    """
    codes = codes[:max_rows]
    rows: list[PickDayReturn] = []
    rets: list[float] = []

    for code in codes:
        try:
            df = engine.get_ohlcv(code)
            if df.empty:
                rows.append(PickDayReturn(code, None))
                continue
            r_next = df[df["date"] == d_next]
            r_next2 = df[df["date"] == d_next2]
            if r_next.empty or r_next2.empty:
                rows.append(PickDayReturn(code, None))
                continue

            o_entry = float(r_next.iloc[0]["open"])
            o_exit = float(r_next2.iloc[0]["open"])

            pct_o: float | None = None
            if o_entry > 0:
                pct_o = (o_exit / o_entry - 1.0) * 100.0
                rets.append(pct_o)

            rows.append(PickDayReturn(code, pct_o))
        except Exception:
            rows.append(PickDayReturn(code, None))

    avg_o = sum(rets) / len(rets) if rets else None
    return rows, avg_o


def format_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}%"
