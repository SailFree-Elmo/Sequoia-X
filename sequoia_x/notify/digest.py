"""多策略命中合并：投票数、Borda 分、Top N 推荐（纯函数，便于测试）。"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class TopPick:
    """综合推荐中的一只 ETF。"""

    code: str
    vote_count: int
    borda_score: float
    strategies: tuple[str, ...]  # 命中策略名，已排序


def aggregate_votes_and_borda(
    strategy_hits: dict[str, list[str]],
) -> dict[str, tuple[int, float, frozenset[str]]]:
    """按策略返回顺序计算每只代码的命中次数与 Borda 分。

    Borda：在某策略中排名第 k（从 1 计）则得 1/k 分。

    Returns:
        code -> (vote_count, borda_sum, frozenset of strategy class names)
    """
    votes: dict[str, int] = defaultdict(int)
    borda: dict[str, float] = defaultdict(float)
    hit_strategies: dict[str, set[str]] = defaultdict(set)

    for strategy_name, ordered in strategy_hits.items():
        for rank, code in enumerate(ordered, start=1):
            votes[code] += 1
            borda[code] += 1.0 / float(rank)
            hit_strategies[code].add(strategy_name)

    return {
        code: (votes[code], borda[code], frozenset(hit_strategies[code]))
        for code in votes
    }


def rank_top_picks(
    strategy_hits: dict[str, list[str]],
    turnover_by_symbol: dict[str, float] | None,
    top_n: int,
) -> list[TopPick]:
    """综合排序后取前 top_n：命中数 > Borda > 当日成交额 > 代码字典序。"""
    agg = aggregate_votes_and_borda(strategy_hits)
    if not agg:
        return []

    to = turnover_by_symbol or {}

    def sort_key(code: str) -> tuple[int, float, float, str]:
        v, b, _strats = agg[code]
        return (v, b, to.get(code, 0.0), code)

    ordered_codes = sorted(agg.keys(), key=sort_key, reverse=True)
    picks: list[TopPick] = []
    for code in ordered_codes[:top_n]:
        v, b, strat_set = agg[code]
        picks.append(
            TopPick(
                code=code,
                vote_count=v,
                borda_score=b,
                strategies=tuple(sorted(strat_set)),
            )
        )
    return picks


def union_symbol_count(strategy_hits: dict[str, list[str]]) -> int:
    """并集中 distinct 代码数量。"""
    u: set[str] = set()
    for lst in strategy_hits.values():
        u.update(lst)
    return len(u)
