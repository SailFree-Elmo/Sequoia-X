"""强势日回踩策略：昨日大涨后今日放量收阴但不破昨收（场内 ETF）。"""

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class EtfStrongPullbackStrategy(BaseStrategy):
    """强势日回踩策略（原涨停洗盘逻辑的 ETF 化）。

    选股条件（向量化，严禁 iterrows）：
    1. 昨日强势：昨日涨幅 >= Settings.strong_day_pct（默认约 3%）
    2. 今日收阴：今日 close < 今日 open
    3. 今日放量：今日 volume > 昨日 volume * 2.0
    4. 支撑不破：今日 low >= 昨日 close

    Attributes:
        webhook_key: 路由到 'shakeout' 专属飞书机器人。
    """

    webhook_key: str = "shakeout"
    _MIN_BARS: int = 3

    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []
        pct = self.settings.strong_day_pct

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue

                prev2 = df.iloc[-3]
                prev1 = df.iloc[-2]
                today = df.iloc[-1]

                strong_yesterday = prev1["close"] >= prev2["close"] * (1 + pct)
                bearish_today = today["close"] < today["open"]
                volume_surge = today["volume"] > prev1["volume"] * 2.0
                support_hold = today["low"] >= prev1["close"]

                if strong_yesterday and bearish_today and volume_surge and support_hold:
                    selected.append(symbol)

            except Exception as exc:
                logger.warning(f"[{symbol}] EtfStrongPullbackStrategy 计算失败：{exc}")
                continue

        logger.info(f"EtfStrongPullbackStrategy 选出 {len(selected)} 只 ETF")
        return selected
