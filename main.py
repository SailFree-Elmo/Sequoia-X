"""Sequoia-X V2 主程序入口（A 股场内 ETF）。

两种运行模式：
  python main.py               # 日常模式：8进程增量补数据 + 跑策略 + 飞书推送
  python main.py --backfill    # 回填模式：baostock 拉全市场 ETF 历史K线（首次/补数据用）
"""

import argparse
import sys
from dotenv import load_dotenv

load_dotenv()

import socket

socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.digest import rank_top_picks
from sequoia_x.notify.feishu import FeishuNotifier
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.etf_dual_ma_trend import EtfDualMaTrendStrategy
from sequoia_x.strategy.etf_multi_factor import EtfMultiFactorStrategy
from sequoia_x.strategy.etf_trend_follow import EtfTrendFollowStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.limit_up_shakeout import EtfStrongPullbackStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import EtfUptrendSharpDropStrategy


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 场内 ETF 筛选系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场 ETF 历史 K 线",
    )
    args = parser.parse_args()

    try:
        settings = get_settings()

        logger = get_logger(__name__)
        logger.info("Sequoia-X V2 启动（场内 ETF）")

        engine = DataEngine(settings)

        if args.backfill:
            logger.info("进入回填模式...")
            all_symbols = engine.get_all_symbols()
            engine.backfill(all_symbols)
            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        logger.info("开始拉取最新快照...")
        count = engine.sync_today_bulk()
        logger.info(f"快照同步完成，写入 {count} 条行情")

        strategies: list[BaseStrategy] = [
            MaVolumeStrategy(engine=engine, settings=settings),
            TurtleTradeStrategy(engine=engine, settings=settings),
            HighTightFlagStrategy(engine=engine, settings=settings),
            EtfStrongPullbackStrategy(engine=engine, settings=settings),
            EtfUptrendSharpDropStrategy(engine=engine, settings=settings),
            RpsBreakoutStrategy(engine=engine, settings=settings),
            EtfDualMaTrendStrategy(engine=engine, settings=settings),
            EtfMultiFactorStrategy(engine=engine, settings=settings),
            EtfTrendFollowStrategy(engine=engine, settings=settings),
        ]

        strategy_hits: dict[str, list[str]] = {}
        for strategy in strategies:
            strategy_name = type(strategy).__name__
            logger.info(f"执行策略：{strategy_name}")
            selected = strategy.run()
            strategy_hits[strategy_name] = selected
            logger.info(f"{strategy_name} 命中 {len(selected)} 只 ETF")

        _log_digest_detail = 20
        for sname, codes in strategy_hits.items():
            head = codes[:_log_digest_detail]
            tail_note = f" …(共{len(codes)}只)" if len(codes) > _log_digest_detail else ""
            logger.info(f"[digest明细] {sname}: {','.join(head)}{tail_note}")

        all_hit_codes: set[str] = set()
        for lst in strategy_hits.values():
            all_hit_codes.update(lst)

        turnover_by_symbol: dict[str, float] = {}
        for sym in all_hit_codes:
            try:
                df = engine.get_ohlcv(sym)
                if len(df) > 0:
                    turnover_by_symbol[sym] = float(df.iloc[-1]["turnover"])
            except Exception:
                continue

        current_asof = engine.get_latest_trade_date()
        prev_asof: str | None = None
        prev_codes: list[str] = []
        if current_asof:
            prev_asof, prev_codes = engine.load_digest_top_picks_strictly_before(current_asof)
            # 与「最新行情日」同一天再次跑时，库中可能仅有 asof==current 的一条，strictly_before 为空；
            # 退化为「按保存时间倒序第二新」的一期做对照（若仅有一条则仍为空，由飞书占位说明）。
            if not prev_codes:
                prev_asof, prev_codes = engine.load_digest_top_picks_second_latest()

        notifier = FeishuNotifier(settings)
        yesterday_sec = notifier.build_yesterday_perf_section(
            engine,
            prev_asof,
            prev_codes,
            current_asof,
            settings.feishu_digest_top_n,
        )

        picks = rank_top_picks(
            strategy_hits,
            turnover_by_symbol if turnover_by_symbol else None,
            top_n=settings.feishu_digest_top_n,
        )
        if current_asof:
            engine.save_digest_top_picks(current_asof, [p.code for p in picks])

        notifier.send_digest(
            strategy_hits,
            turnover_by_symbol=turnover_by_symbol if turnover_by_symbol else None,
            yesterday_section=yesterday_sec,
        )

    except Exception:
        try:
            _logger = get_logger(__name__)
            _logger.exception("主流程发生未捕获异常，程序终止")
        except Exception:
            import traceback

            traceback.print_exc()
        sys.exit(1)

    logger.info("Sequoia-X V2 运行完成")


if __name__ == "__main__":
    main()
