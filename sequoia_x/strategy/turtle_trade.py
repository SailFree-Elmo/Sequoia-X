"""海龟交易策略：20日新高突破 + 成交额阈值 + 动量阳线过滤（场内 ETF）。"""

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class TurtleTradeStrategy(BaseStrategy):
    """海龟突破策略（ETF 版）。

    选股条件（向量化，严禁 iterrows）：
    1. 突破新高：今日 close > 前20个交易日 high 的最大值
    2. 流动性：今日 turnover >= Settings.turtle_min_turnover
    3. 防诱多过滤：今日必须是实体阳线（今日 close > 今日 open），且必须真涨（今日 close > 昨日 close）

    排序：按当日成交额从高到低。

    Attributes:
        webhook_key: 路由到 'turtle' 专属飞书机器人。
    """

    webhook_key: str = "turtle"
    _MIN_BARS: int = 21  # 至少需要 21 根 K 线（20日窗口 + 当日）

    def run(self) -> list[str]:
        """遍历本地 ETF 池，返回满足海龟突破条件的代码列表。"""
        symbols = self.engine.get_local_symbols()
        candidates: list[str] = []

        min_turnover = self.settings.turtle_min_turnover

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue

                df["high_20"] = df["high"].shift(1).rolling(20).max()

                last = df.iloc[-1]
                prev = df.iloc[-2]

                if pd.isna(last["high_20"]):
                    continue

                breakout = last["close"] > last["high_20"]
                liquid = last["turnover"] >= min_turnover
                is_yang = last["close"] > last["open"]
                is_up = last["close"] > prev["close"]

                if breakout and liquid and is_yang and is_up:
                    candidates.append(symbol)

            except Exception as exc:
                logger.warning(f"[{symbol}] TurtleTradeStrategy 计算失败：{exc}")
                continue

        if candidates:
            turnovers: dict[str, float] = {}
            for sym in candidates:
                try:
                    df = self.engine.get_ohlcv(sym)
                    turnovers[sym] = float(df.iloc[-1]["turnover"])
                except Exception:
                    turnovers[sym] = 0.0
            candidates.sort(key=lambda s: turnovers.get(s, 0.0), reverse=True)

        logger.info(f"TurtleTradeStrategy 选出 {len(candidates)} 只 ETF")
        return candidates
