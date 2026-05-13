"""多策略命中合并：投票数、Borda 分、Top N 推荐（纯函数，便于测试）。"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class TopPick:
    """综合推荐中的一只 ETF。"""

    code: str
    vote_count: int
    vote_score: float
    borda_score: float
    strategies: tuple[str, ...]  # 命中策略名，已排序


def aggregate_votes_and_borda(
    strategy_hits: dict[str, list[str]],
) -> dict[str, tuple[int, float, float, frozenset[str]]]:
    """按策略返回顺序计算每只代码的命中次数与 Borda 分。

    Borda：在某策略中排名第 k（从 1 计）则得 1/k 分。

    Returns:
        code -> (vote_count, borda_sum, frozenset of strategy class names)
    """
    votes: dict[str, int] = defaultdict(int)
    vote_score: dict[str, float] = defaultdict(float)
    borda: dict[str, float] = defaultdict(float)
    hit_strategies: dict[str, set[str]] = defaultdict(set)

    for strategy_name, ordered in strategy_hits.items():
        for rank, code in enumerate(ordered, start=1):
            votes[code] += 1
            vote_score[code] += 1.0
            borda[code] += 1.0 / float(rank)
            hit_strategies[code].add(strategy_name)

    return {
        code: (votes[code], vote_score[code], borda[code], frozenset(hit_strategies[code]))
        for code in votes
    }


def rank_top_picks(
    strategy_hits: dict[str, list[str]],
    turnover_by_symbol: dict[str, float] | None,
    top_n: int,
    *,
    strategy_weights: dict[str, float] | None = None,
    strategy_groups: dict[str, str] | None = None,
    group_multipliers: dict[str, float] | None = None,
) -> list[TopPick]:
    """综合排序后取前 top_n：原始命中数 > 加权票分+加权Borda > 当日成交额 > 代码字典序。"""
    agg = _aggregate_weighted(
        strategy_hits,
        strategy_weights=strategy_weights,
        strategy_groups=strategy_groups,
        group_multipliers=group_multipliers,
    )
    if not agg:
        return []

    to = turnover_by_symbol or {}

    def sort_key(code: str) -> tuple[int, float, float, str]:
        v_raw, v_score, b, _strats = agg[code]
        return (v_raw, v_score + b, to.get(code, 0.0), code)

    ordered_codes = sorted(agg.keys(), key=sort_key, reverse=True)
    picks: list[TopPick] = []
    for code in ordered_codes[:top_n]:
        v_raw, v_score, b, strat_set = agg[code]
        picks.append(
            TopPick(
                code=code,
                vote_count=v_raw,
                vote_score=v_score,
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


def _aggregate_weighted(
    strategy_hits: dict[str, list[str]],
    *,
    strategy_weights: dict[str, float] | None,
    strategy_groups: dict[str, str] | None,
    group_multipliers: dict[str, float] | None,
) -> dict[str, tuple[int, float, float, frozenset[str]]]:
    votes: dict[str, int] = defaultdict(int)
    vote_score: dict[str, float] = defaultdict(float)
    borda: dict[str, float] = defaultdict(float)
    hit_strategies: dict[str, set[str]] = defaultdict(set)

    for strategy_name, ordered in strategy_hits.items():
        w = 1.0
        if strategy_weights:
            w = float(strategy_weights.get(strategy_name, 1.0))
        if strategy_groups and group_multipliers:
            grp = strategy_groups.get(strategy_name, "")
            w = w * float(group_multipliers.get(grp, 1.0))
        for rank, code in enumerate(ordered, start=1):
            votes[code] += 1
            vote_score[code] += w
            borda[code] += w * (1.0 / float(rank))
            hit_strategies[code].add(strategy_name)

    return {
        code: (votes[code], vote_score[code], borda[code], frozenset(hit_strategies[code]))
        for code in votes
    }
