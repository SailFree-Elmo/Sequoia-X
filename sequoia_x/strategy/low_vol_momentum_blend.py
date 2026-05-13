"""低波动动量融合：动量强度 + 波动惩罚。"""

from __future__ import annotations

import pandas as pd

from sequoia_x.core.logger import get_logger
from sequoia_x.strategy.base import BaseStrategy

logger = get_logger(__name__)


class LowVolMomentumBlendStrategy(BaseStrategy):
    webhook_key: str = "low_vol_momentum"
    _MIN_BARS: int = 100

    def run(self) -> list[str]:
        symbols = self.engine.get_local_symbols()
        rows: list[dict[str, float | str]] = []
        min_turn20 = self.settings.lvmb_min_turnover_20d
        max_vol20 = self.settings.lvmb_max_volatility_20d
        max_r5 = self.settings.lvmb_max_5d_return_pct
        max_out = max(1, int(self.settings.lvmb_max_results))

        for symbol in symbols:
            try:
                df = self.engine.get_ohlcv(symbol)
                if len(df) < self._MIN_BARS:
                    continue
                c = df["close"].astype(float)
                h = df["high"].astype(float)
                l = df["low"].astype(float)
                to = df["turnover"].astype(float)

                ma20 = c.rolling(20).mean()
                ma60 = c.rolling(60).mean()
                c0 = float(c.iloc[-1])
                ma20v = float(ma20.iloc[-1])
                ma60v = float(ma60.iloc[-1])
                if pd.isna(ma20v) or pd.isna(ma60v):
                    continue
                if not (c0 > ma20v > ma60v):
                    continue

                c5 = float(c.iloc[-6])
                c20 = float(c.iloc[-21])
                c60 = float(c.iloc[-61])
                if c5 <= 0 or c20 <= 0 or c60 <= 0:
                    continue
                r5 = c0 / c5 - 1.0
                if r5 > max_r5:
                    continue
                r20 = c0 / c20 - 1.0
                r60 = c0 / c60 - 1.0
                if r20 <= 0 or r60 <= 0:
                    continue

                rets = c.pct_change().iloc[-20:]
                vol20 = float(rets.std())
                if pd.isna(vol20) or vol20 <= 0 or vol20 > max_vol20:
                    continue

                tr = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(
                    axis=1
                )
                atr14 = float(tr.rolling(14).mean().iloc[-1])
                atr_ratio = atr14 / c0 if c0 > 0 else 1.0

                turn20 = float(to.iloc[-20:].mean())
                if turn20 < min_turn20:
                    continue

                rows.append(
                    {
                        "symbol": symbol,
                        "mom20": r20,
                        "mom60": r60,
                        "vol20": vol20,
                        "atr_ratio": atr_ratio,
                        "turn20": turn20,
                    }
                )
            except Exception as exc:
                logger.warning(f"[{symbol}] LowVolMomentumBlendStrategy 计算失败：{exc}")
                continue

        if not rows:
            logger.info("LowVolMomentumBlendStrategy 选出 0 只 ETF")
            return []
        panel = pd.DataFrame(rows)
        panel["score"] = (
            panel["mom20"].rank(pct=True) * 0.40
            + panel["mom60"].rank(pct=True) * 0.35
            + panel["turn20"].rank(pct=True) * 0.10
            + (1.0 - panel["vol20"].rank(pct=True)) * 0.10
            + (1.0 - panel["atr_ratio"].rank(pct=True)) * 0.05
        )
        panel = panel.sort_values("score", ascending=False).head(max_out)
        out = panel["symbol"].astype(str).tolist()
        logger.info(f"LowVolMomentumBlendStrategy 选出 {len(out)} 只 ETF")
        return out
