"""消息面情绪扩散策略（依赖外部新闻信号文件）。"""

from __future__ import annotations

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.data.news_adapter import NewsDataAdapter
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class NewsSentimentBreadthStrategy(BaseStrategy):
    webhook_key: str = "news_sentiment"

    def run(self) -> list[str]:
        lookback = max(1, int(self.settings.news_lookback_days))
        sent_th = self.settings.news_sentiment_threshold
        heat_accel_th = self.settings.news_heat_accel_threshold

        adapter = NewsDataAdapter(self.settings.news_signal_path)
        rows = adapter.load_recent_signals(lookback)
        if not rows:
            logger.info("NewsSentimentBreadthStrategy 选出 0 只 ETF（无消息信号）")
            return []

        panel = pd.DataFrame(
            [{"symbol": r.symbol, "date": r.date, "sentiment": r.sentiment, "heat": r.heat} for r in rows]
        )
        panel = panel.sort_values(["symbol", "date"])
        grp = panel.groupby("symbol", as_index=False).agg(
            sentiment_mean=("sentiment", "mean"),
            heat_last=("heat", "last"),
            heat_first=("heat", "first"),
        )
        grp["heat_accel"] = grp["heat_last"] - grp["heat_first"]
        picked = grp[
            (grp["sentiment_mean"] >= sent_th) & (grp["heat_accel"] >= heat_accel_th)
        ].copy()
        picked["score"] = picked["sentiment_mean"] * 0.7 + picked["heat_accel"] * 0.3
        picked = picked.sort_values("score", ascending=False)
        out = picked["symbol"].astype(str).tolist()
        logger.info(f"NewsSentimentBreadthStrategy 选出 {len(out)} 只 ETF")
        return out
