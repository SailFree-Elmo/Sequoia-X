"""双动量轮动：相对动量 + 绝对动量过滤。"""

from __future__ import annotations

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class DualMomentumRotationStrategy(BaseStrategy):
    """日频双动量轮动策略。"""

    webhook_key: str = "dual_momentum_rotation"
    _MIN_BARS: int = 90

    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        rows: list[dict[str, float | str]] = []
        min_turn20 = self.settings.dmr_min_turnover_20d
        max_5d = self.settings.dmr_max_5d_return_pct
        max_out = max(1, int(self.settings.dmr_max_results))

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue

                c = df["close"].astype(float)
                to = df["turnover"].astype(float)
                ma60 = c.rolling(60).mean()

                c0 = float(c.iloc[-1])
                c5 = float(c.iloc[-6])
                c20 = float(c.iloc[-21])
                c60 = float(c.iloc[-61])
                ma60v = float(ma60.iloc[-1])
                if c5 <= 0 or c20 <= 0 or c60 <= 0 or pd.isna(ma60v):
                    continue

                # 绝对动量：价格需在长期均线上方
                if c0 <= ma60v:
                    continue

                r5 = c0 / c5 - 1.0
                if r5 > max_5d:
                    continue
                r20 = c0 / c20 - 1.0
                r60 = c0 / c60 - 1.0
                if r20 <= 0 or r60 <= 0:
                    continue

                turn20 = float(to.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                rows.append(
                    {
                        "symbol": symbol,
                        "r20": r20,
                        "r60": r60,
                        "turn20": turn20,
                    }
                )
            except Exception as exc:
                logger.warning(f"[{symbol}] DualMomentumRotationStrategy 计算失败：{exc}")
                continue

        if not rows:
            logger.info("DualMomentumRotationStrategy 选出 0 只 ETF")
            return []

        panel = pd.DataFrame(rows)
        panel["score"] = (
            panel["r20"].rank(pct=True) * 0.45
            + panel["r60"].rank(pct=True) * 0.45
            + panel["turn20"].rank(pct=True) * 0.10
        )
        panel = panel.sort_values("score", ascending=False).head(max_out)
        out = panel["symbol"].astype(str).tolist()
        logger.info(f"DualMomentumRotationStrategy 选出 {len(out)} 只 ETF")
        return out
