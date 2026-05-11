"""上升趋势大单日大跌策略：趋势中放量急跌，捕捉错杀（场内 ETF）。"""

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class EtfUptrendSharpDropStrategy(BaseStrategy):
    """上升趋势中的单日大跌策略（ETF 版）。

    选股条件（向量化，严禁 iterrows）：
    1. 处于上升趋势：昨日20日均线 > 昨日60日均线
    2. 放量急跌：今日跌幅 >= Settings.sharp_drop_pct（相对昨日收盘）
                且今日 volume > 20日均量的 2.0 倍

    Attributes:
        webhook_key: 路由到 'limit_down' 专属飞书机器人。
    """

    webhook_key: str = "limit_down"
    _MIN_BARS: int = 60

    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []
        drop_pct = self.settings.sharp_drop_pct

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue

                df["ma20"] = df["close"].rolling(20).mean()
                df["ma60"] = df["close"].rolling(60).mean()
                df["vol_ma20"] = df["volume"].rolling(20).mean()

                prev = df.iloc[-2]
                today = df.iloc[-1]

                if pd.isna(prev["ma20"]) or pd.isna(prev["ma60"]) or pd.isna(today["vol_ma20"]):
                    continue

                uptrend = prev["ma20"] > prev["ma60"]
                sharp_drop = today["close"] <= prev["close"] * (1 - drop_pct)
                volume_surge = today["volume"] > today["vol_ma20"] * 2.0

                if uptrend and sharp_drop and volume_surge:
                    selected.append(symbol)

            except Exception as exc:
                logger.warning(f"[{symbol}] EtfUptrendSharpDropStrategy 计算失败：{exc}")
                continue

        logger.info(f"EtfUptrendSharpDropStrategy 选出 {len(selected)} 只 ETF")
        return selected
