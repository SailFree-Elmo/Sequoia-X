"""合并日报：投票、Borda、Top N 排序。"""

from sequoia_x.notify.digest import (
    aggregate_votes_and_borda,
    rank_top_picks,
    union_symbol_count,
)


def test_union_and_votes() -> None:
    hits = {
        "S1": ["111111", "222222"],
        "S2": ["222222", "333333"],
    }
    assert union_symbol_count(hits) == 3
    agg = aggregate_votes_and_borda(hits)
    assert agg["222222"][0] == 2  # votes
    assert agg["111111"][0] == 1
    assert agg["333333"][0] == 1


def test_borda_same_votes_tiebreak_turnover() -> None:
    """两代码各命中 2 条策略时，Borda 高者在前；再平则用成交额。"""
    hits = {
        "A": ["111111", "222222"],  # 111 rank1, 222 rank2 -> borda 1, 0.5
        "B": ["222222", "111111"],  # 222 rank1, 111 rank2 -> 222 gets 1+0.5, 111 gets 0.5+1
    }
    agg = aggregate_votes_and_borda(hits)
    # 111111: 1 + 0.5 = 1.5, 222222: 0.5 + 1 = 1.5
    assert agg["111111"][1] == agg["222222"][1] == 1.5

    to_low = {"111111": 100.0, "222222": 200.0}
    top = rank_top_picks(hits, to_low, top_n=2)
    assert [p.code for p in top] == ["222222", "111111"]

    to_high_first = {"111111": 300.0, "222222": 200.0}
    top2 = rank_top_picks(hits, to_high_first, top_n=2)
    assert [p.code for p in top2] == ["111111", "222222"]


def test_rank_top_picks_order_by_votes_then_borda() -> None:
    hits = {
        "Ma": ["100000", "200000", "300000"],
        "Tu": ["300000"],
    }
    # 300000: votes 2, borda 1 + 1/3
    # 100000: votes 1, borda 1
    # 200000: votes 1, borda 0.5
    top = rank_top_picks(hits, None, top_n=10)
    codes = [p.code for p in top]
    assert codes == ["300000", "100000", "200000"]


def test_empty_hits() -> None:
    assert union_symbol_count({}) == 0
    assert rank_top_picks({}, None, top_n=10) == []
    assert aggregate_votes_and_borda({"X": [], "Y": []}) == {}
