"""Sequoia-X V2 主程序入口（A 股场内 ETF）。

两种运行模式：
  python main.py               # 日常模式：8进程增量补数据 + 跑策略 + 飞书推送
  python main.py --backfill    # 回填模式：baostock 拉全市场 ETF 历史K线（首次/补数据用）
  python main.py --backfill-range 2023-01-01 2023-12-31  # 仅写入该日历区间（后复权 upsert；baostock 无数据时回退 akshare）
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

import socket

socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.digest import rank_top_picks
from sequoia_x.notify.feishu import FeishuNotifier
from sequoia_x.strategy.adx_ma_regime_trend import AdxMaRegimeTrendStrategy
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.dual_momentum_rotation import DualMomentumRotationStrategy
from sequoia_x.strategy.etf_dual_ma_trend import EtfDualMaTrendStrategy
from sequoia_x.strategy.etf_multi_factor import EtfMultiFactorStrategy
from sequoia_x.strategy.etf_trend_follow import EtfTrendFollowStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.industry_relative_strength_rotation import (
    IndustryRelativeStrengthRotationStrategy,
)
from sequoia_x.strategy.limit_up_shakeout import EtfStrongPullbackStrategy
from sequoia_x.strategy.low_vol_momentum_blend import LowVolMomentumBlendStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.market_regime_filter import MarketRegimeFilter
from sequoia_x.strategy.news_sentiment_breadth import NewsSentimentBreadthStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy
from sequoia_x.strategy.strong_trend_low_chase import StrongTrendLowChaseStrategy
from sequoia_x.strategy.trend_stability_momentum import TrendStabilityMomentumStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import EtfUptrendSharpDropStrategy
from sequoia_x.strategy.volume_contraction_breakout import VolumeContractionBreakoutStrategy


def resolve_push_mode(requested: str, now: datetime | None = None) -> str:
    if requested in {"morning", "close", "intraday"}:
        return requested
    t = now or datetime.now()
    if t.hour < 9:
        return "morning"
    if t.hour >= 15:
        return "close"
    return "intraday"


def build_aggressive_settings(base_settings):
    """基于当前配置生成激进版参数快照。"""
    return base_settings.model_copy(
        deep=True,
        update={
            # 反转类与稳健默认一致（由 .env 打开）；减轻激进画像全市场扫描
            "stlc_max_5d_return_pct": 0.14,
            "stlc_max_distance_from_ma20": 0.12,
            "stlc_max_upper_shadow_ratio": 0.45,
            "stlc_min_close_position_ratio": 0.55,
            "vcb_contraction_max_range_ratio": 0.12,
            "vcb_volume_breakout_multiplier": 1.6,
            "vcb_max_5d_return_pct": 0.14,
            "vcb_min_close_position_ratio": 0.55,
            "industry_rotation_top_groups": 5,
            "industry_rotation_pick_per_group": 2,
            "industry_rotation_max_5d_return_pct": 0.14,
            "dmr_max_5d_return_pct": 0.16,
            "tsm_max_5d_return_pct": 0.16,
            "lvmb_max_volatility_20d": 0.05,
        },
    )


def run_profile_strategies(
    engine,
    settings,
    regime,
    logger,
    profile_name: str,
    *,
    quiet: bool = False,
) -> dict[str, list[str]]:
    """运行单个参数画像下的策略集合。

    Args:
        quiet: 为 True 时降低日志粒度（回测批跑用），避免逐策略 INFO 刷屏。
    """
    all_strategies: list[BaseStrategy] = [
        MaVolumeStrategy(engine=engine, settings=settings),
        TurtleTradeStrategy(engine=engine, settings=settings),
        HighTightFlagStrategy(engine=engine, settings=settings),
        EtfStrongPullbackStrategy(engine=engine, settings=settings),
        EtfUptrendSharpDropStrategy(engine=engine, settings=settings),
        RpsBreakoutStrategy(engine=engine, settings=settings),
        EtfDualMaTrendStrategy(engine=engine, settings=settings),
        EtfMultiFactorStrategy(engine=engine, settings=settings),
        EtfTrendFollowStrategy(engine=engine, settings=settings),
        StrongTrendLowChaseStrategy(engine=engine, settings=settings),
        AdxMaRegimeTrendStrategy(engine=engine, settings=settings),
        VolumeContractionBreakoutStrategy(engine=engine, settings=settings),
        IndustryRelativeStrengthRotationStrategy(engine=engine, settings=settings),
        NewsSentimentBreadthStrategy(engine=engine, settings=settings),
        DualMomentumRotationStrategy(engine=engine, settings=settings),
        TrendStabilityMomentumStrategy(engine=engine, settings=settings),
        LowVolMomentumBlendStrategy(engine=engine, settings=settings),
    ]

    strategies: list[BaseStrategy] = []
    groups = settings.get_strategy_groups()
    for strategy in all_strategies:
        key = strategy.webhook_key
        strategy_name = type(strategy).__name__
        if not settings.is_strategy_enabled(key):
            if not quiet:
                logger.info("[%s] 跳过策略 %s：配置关闭", profile_name, strategy_name)
            continue
        group = groups.get(strategy_name, "trend")
        if (
            group == "reversal"
            and (not regime.is_risk_on)
            and (not settings.regime_allow_reversal_when_risk_off)
        ):
            if not quiet:
                logger.info("[%s] 跳过策略 %s：risk_off 下关闭反转策略", profile_name, strategy_name)
            continue
        strategies.append(strategy)

    strategy_hits: dict[str, list[str]] = {}
    for strategy in strategies:
        strategy_name = type(strategy).__name__
        if not quiet:
            logger.info("[%s] 执行策略：%s", profile_name, strategy_name)
        selected = strategy.run()
        strategy_hits[strategy_name] = selected
        if not quiet:
            logger.info("[%s] %s 命中 %d 只 ETF", profile_name, strategy_name, len(selected))
    return strategy_hits


def estimate_recent_bars_for_profiles(settings) -> int:
    """估算策略批跑需要的最近 K 线窗口，避免加载全历史数据。"""
    candidates = [
        120,  # 绝大多数策略窗口上界
        settings.rps_period + 5,
        settings.regime_ma_window + 60,
        settings.etf_dual_ma_confirm_days + 70,
        settings.tsm_lookback_days + 90,
        260,  # 兜底：保留约 1 年交易日
    ]
    return max(candidates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sequoia-X V2 场内 ETF 筛选系统")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="回填模式：通过 baostock 拉取全市场 ETF 历史 K 线",
    )
    parser.add_argument(
        "--backfill-range",
        nargs=2,
        metavar=("START", "END"),
        help="仅回填 START～END 日历区间内的日 K（后复权 upsert），例如 2023-01-01 2023-12-31",
    )
    parser.add_argument(
        "--push-mode",
        choices=["auto", "morning", "close", "intraday"],
        default="auto",
        help="飞书推送模式：auto/morning/close/intraday",
    )
    args = parser.parse_args()

    try:
        settings = get_settings()
        push_mode = resolve_push_mode(
            args.push_mode if args.push_mode else settings.feishu_push_mode_default
        )

        logger = get_logger(__name__)
        logger.info("Sequoia-X V2 启动（场内 ETF）")
        logger.info(f"飞书推送模式：{push_mode}")

        engine = DataEngine(settings)

        if args.backfill_range:
            start_r, end_r = args.backfill_range
            logger.info("进入区间回填模式：%s ~ %s", start_r, end_r)
            all_symbols = engine.get_all_symbols()
            engine.backfill_date_range(all_symbols, start_r, end_r)
            logger.info("Sequoia-X V2 区间回填运行完成")
            return

        if args.backfill:
            logger.info("进入回填模式...")
            all_symbols = engine.get_all_symbols()
            engine.backfill(all_symbols)
            logger.info("Sequoia-X V2 回填模式运行完成")
            return

        logger.info("开始拉取最新快照...")
        count = engine.sync_today_bulk()
        logger.info(f"快照同步完成，写入 {count} 条行情")
        recent_bars = estimate_recent_bars_for_profiles(settings)
        warmed = engine.preload_ohlcv_cache(recent_bars=recent_bars)
        logger.info("预热运行期缓存完成：%d symbols, recent_bars=%d", warmed, recent_bars)

        regime_filter = MarketRegimeFilter(engine=engine, settings=settings)
        regime = regime_filter.detect()
        logger.info(
            "市场状态门控：%s (bench=%.3f breadth=%.3f)",
            regime.regime,
            regime.benchmark_strength_ratio,
            regime.breadth_ratio,
        )

        aggressive_settings = build_aggressive_settings(settings)
        with ThreadPoolExecutor(max_workers=2) as executor:
            conservative_future = executor.submit(
                run_profile_strategies,
                engine,
                settings,
                regime,
                logger,
                "稳健版",
            )
            aggressive_future = executor.submit(
                run_profile_strategies,
                engine,
                aggressive_settings,
                regime,
                logger,
                "激进版",
            )
            strategy_hits = conservative_future.result()
            strategy_hits_aggressive = aggressive_future.result()

        _log_digest_detail = 20
        for sname, codes in strategy_hits.items():
            head = codes[:_log_digest_detail]
            tail_note = f" …(共{len(codes)}只)" if len(codes) > _log_digest_detail else ""
            logger.info(f"[稳健版 digest明细] {sname}: {','.join(head)}{tail_note}")
        for sname, codes in strategy_hits_aggressive.items():
            head = codes[:_log_digest_detail]
            tail_note = f" …(共{len(codes)}只)" if len(codes) > _log_digest_detail else ""
            logger.info(f"[激进版 digest明细] {sname}: {','.join(head)}{tail_note}")

        all_hit_codes: set[str] = set()
        for lst in strategy_hits.values():
            all_hit_codes.update(lst)
        for lst in strategy_hits_aggressive.values():
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
        prev_stats: dict[str, tuple[int, float]] = {}
        if current_asof:
            prev_asof, prev_codes, prev_stats = engine.load_digest_top_picks_strictly_before(current_asof)
            # 与「最新行情日」同一天再次跑时，库中可能仅有 asof==current 的一条，strictly_before 为空；
            # 退化为「按保存时间倒序第二新」的一期做对照（若仅有一条则仍为空，由飞书占位说明）。
            if not prev_codes:
                prev_asof, prev_codes, prev_stats = engine.load_digest_top_picks_second_latest()

        notifier = FeishuNotifier(settings)
        yesterday_sec = notifier.build_yesterday_perf_section(
            engine,
            prev_asof,
            prev_codes,
            prev_stats,
            current_asof,
            settings.feishu_digest_top_n,
            detailed=(push_mode == "close"),
        )

        picks = rank_top_picks(
            strategy_hits,
            turnover_by_symbol if turnover_by_symbol else None,
            top_n=settings.feishu_digest_top_n,
            strategy_weights=settings.get_strategy_weights(),
            strategy_groups=settings.get_strategy_groups(),
            group_multipliers=settings.get_regime_group_multipliers(regime.regime),
        )
        if current_asof:
            pick_rows = [
                {"code": p.code, "vote_count": p.vote_count, "vote_score": float(p.vote_score)}
                for p in picks
            ]
            engine.save_digest_top_picks(
                current_asof, [p.code for p in picks], pick_rows=pick_rows
            )

        notifier.send_digest(
            strategy_hits,
            turnover_by_symbol=turnover_by_symbol if turnover_by_symbol else None,
            yesterday_section=yesterday_sec,
            strategy_weights=settings.get_strategy_weights(),
            strategy_groups=settings.get_strategy_groups(),
            group_multipliers=settings.get_regime_group_multipliers(regime.regime),
            push_mode=push_mode,
            asof_date=current_asof,
            strategy_hits_alt=strategy_hits_aggressive,
            strategy_weights_alt=aggressive_settings.get_strategy_weights(),
            strategy_groups_alt=aggressive_settings.get_strategy_groups(),
            group_multipliers_alt=aggressive_settings.get_regime_group_multipliers(regime.regime),
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
