"""双均线多头排列趋势策略：连续多日 MA20>MA60 且收盘站上 MA20（场内 ETF）。"""

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class EtfDualMaTrendStrategy(BaseStrategy):
    """双均线趋势确认策略。

    在最近 ``etf_dual_ma_confirm_days`` 个交易日内，每日均满足 MA20 > MA60；
    且最新收盘价高于当日 MA20；成交额不低于 ``etf_dual_ma_min_turnover``。

    Attributes:
        webhook_key: 路由到 'etf_dual_ma' 专属飞书机器人。
    """

    webhook_key: str = "etf_dual_ma"
    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        selected: list[str] = []
        n = self.settings.etf_dual_ma_confirm_days
        min_to = self.settings.etf_dual_ma_min_turnover

        need_bars = 60 + n
        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < need_bars:
                    continue

                df = df.copy()
                df["ma20"] = df["close"].rolling(20).mean()
                df["ma60"] = df["close"].rolling(60).mean()

                tail = df.iloc[-n:]
                if tail[["ma20", "ma60"]].isna().any().any():
                    continue
                if not (tail["ma20"] > tail["ma60"]).all():
                    continue

                last = df.iloc[-1]
                if last["close"] <= last["ma20"]:
                    continue
                if last["turnover"] < min_to:
                    continue

                selected.append(symbol)

            except Exception as exc:
                logger.warning(f"[{symbol}] EtfDualMaTrendStrategy 计算失败：{exc}")
                continue

        logger.info(f"EtfDualMaTrendStrategy 选出 {len(selected)} 只 ETF")
        return selected
