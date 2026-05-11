import pandas as pd
import sqlite3

from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


class RpsBreakoutStrategy(BaseStrategy):
    """RPS 极强动量突破策略（在场内 ETF 池内横向排名）。"""

    webhook_key: str = "rps"

    def run(self) -> list[str]:
        period = self.settings.rps_period
        threshold = self.settings.rps_threshold

        try:
            with sqlite3.connect(self.engine.db_path) as conn:
                df = pd.read_sql("SELECT symbol, date, close, high FROM stock_daily", conn)
        except Exception as exc:
            logger.error(f"读取数据库失败: {exc}")
            return []

        if df.empty:
            return []

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["symbol", "date"])

        df["close_shift"] = df.groupby("symbol")["close"].shift(period)
        df["pct_change"] = (df["close"] - df["close_shift"]) / df["close_shift"]

        latest_date = df["date"].max()
        latest_df = df[df["date"] == latest_date].copy()
        latest_df = latest_df.dropna(subset=["pct_change"])

        latest_df["rps"] = latest_df["pct_change"].rank(pct=True) * 100
        strong = latest_df[latest_df["rps"] >= threshold].copy()

        roll_high = df.groupby("symbol")["high"].rolling(
            window=period, min_periods=period // 2
        ).max().reset_index(level=0, drop=True)
        df["roll_high"] = roll_high

        latest_roll_high = df[df["date"] == latest_date][["symbol", "roll_high"]]
        strong = strong.merge(latest_roll_high, on="symbol")

        breakout_condition = strong["close"] >= strong["roll_high"] * 0.90
        selected = strong[breakout_condition]

        logger.info(f"RpsBreakoutStrategy 选出 {len(selected)} 只 ETF")
        return selected["symbol"].tolist()
