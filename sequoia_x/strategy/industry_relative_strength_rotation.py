"""行业/主题相对强弱轮动策略。"""

from __future__ import annotations

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


def _industry_bucket(symbol: str) -> str:
    # 在缺少行业映射表时，先按 ETF 前缀分桶，后续可替换为真实行业映射。
    if symbol.startswith(("512", "515", "516", "561", "562")):
        return f"industry_{symbol[:3]}"
    if symbol.startswith(("159", "588")):
        return f"theme_{symbol[:3]}"
    return "broad_market"


class IndustryRelativeStrengthRotationStrategy(BaseStrategy):
    webhook_key: str = "industry_rotation"
    _MIN_BARS: int = 80

    def run(self) -> list[str]:
        min_turn20 = self.settings.industry_rotation_min_turnover_20d
        top_groups = max(1, int(self.settings.industry_rotation_top_groups))
        pick_per_group = max(1, int(self.settings.industry_rotation_pick_per_group))
        max_5d = self.settings.industry_rotation_max_5d_return_pct

        symbols = self.engine.get_local_symbols()
        rows: list[dict[str, float | str]] = []
        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue
                c = df["close"].astype(float)
                to = df["turnover"].astype(float)
                c0 = float(c.iloc[-1])
                c5 = float(c.iloc[-6])
                c20 = float(c.iloc[-21])
                c60 = float(c.iloc[-61])
                if c5 <= 0 or c20 <= 0 or c60 <= 0:
                    continue
                r5 = c0 / c5 - 1.0
                if r5 > max_5d:
                    continue
                turn20 = float(to.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue
                r20 = c0 / c20 - 1.0
                r60 = c0 / c60 - 1.0
                rows.append(
                    {
                        "symbol": symbol,
                        "group": _industry_bucket(symbol),
                        "r20": r20,
                        "r60": r60,
                        "r5": r5,
                        "turn20": turn20,
                    }
                )
            except Exception as exc:
                logger.warning(f"[{symbol}] IndustryRelativeStrengthRotationStrategy 计算失败：{exc}")
                continue

        if not rows:
            logger.info("IndustryRelativeStrengthRotationStrategy 选出 0 只 ETF")
            return []

        panel = pd.DataFrame(rows)
        group_score = (
            panel.groupby("group", as_index=False)
            .agg(r20_mean=("r20", "mean"), r60_mean=("r60", "mean"))
            .assign(score=lambda d: d["r20_mean"] * 0.6 + d["r60_mean"] * 0.4)
            .sort_values("score", ascending=False)
        )
        winners = set(group_score.head(top_groups)["group"].astype(str).tolist())
        panel = panel[panel["group"].isin(winners)].copy()
        panel["symbol_score"] = (
            panel["r20"] * 0.5
            + panel["r60"] * 0.3
            + panel["turn20"].rank(pct=True) * 0.10
            + (1.0 - panel["r5"].rank(pct=True)) * 0.10
        )

        out: list[str] = []
        for g in group_score["group"]:
            if g not in winners:
                continue
            sub = panel[panel["group"] == g].sort_values("symbol_score", ascending=False)
            out.extend(sub.head(pick_per_group)["symbol"].astype(str).tolist())

        logger.info(f"IndustryRelativeStrengthRotationStrategy 选出 {len(out)} 只 ETF")
        return out
