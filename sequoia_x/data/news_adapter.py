"""消息面数据适配层：读取新闻情绪信号并提供统一查询接口。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class NewsSignal:
    symbol: str
    date: str
    sentiment: float
    heat: float


class NewsDataAdapter:
    """默认实现：从 JSON 文件读取消息面信号。"""

    def __init__(self, signal_path: str) -> None:
        self.signal_path = signal_path

    def load_recent_signals(self, lookback_days: int) -> list[NewsSignal]:
        if not self.signal_path:
            return []

        p = Path(self.signal_path)
        if not p.exists():
            return []

        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return []

        if not isinstance(payload, list):
            return []

        now = datetime.utcnow()
        out: list[NewsSignal] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip()
            d = str(item.get("date", "")).strip()
            if len(symbol) != 6 or not symbol.isdigit() or not d:
                continue
            try:
                dt = datetime.fromisoformat(d)
            except ValueError:
                continue
            if (now - dt).days > lookback_days:
                continue
            try:
                sentiment = float(item.get("sentiment", 0.0))
                heat = float(item.get("heat", 0.0))
            except Exception:
                continue
            out.append(NewsSignal(symbol=symbol, date=d, sentiment=sentiment, heat=heat))
        return out
