"""配置管理模块：通过 pydantic-settings 从环境变量或 .env 文件加载系统配置。"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_path: str = "data/etf_sequoia.db"
    start_date: str = "2024-01-01"
    feishu_webhook_url: str  # 必填字段，缺失时抛出 ValidationError
    strategy_webhooks: dict[str, str] = {}

    # ETF 策略阈值（可通过环境变量覆盖，字段名大写下划线）
    turtle_min_turnover: float = 30_000_000.0  # 海龟策略：最小成交额（元）
    strong_day_pct: float = 0.03  # 强势日涨幅下限（小数，如 0.03 表示 3%）
    sharp_drop_pct: float = 0.03  # 上升趋势中单日大跌幅度（小数）
    high_tight_momentum_ratio: float = 1.3  # 高窄旗形：40 日区间强弱比下限
    ma_volume_surge_multiplier: float = 1.5  # 均线放量：相对 20 日均量倍数
    rps_period: int = 120  # RPS 回看交易日数
    rps_threshold: int = 90  # RPS 百分位下限
    etf_dual_ma_min_turnover: float = 5_000_000.0  # 双均线趋势：最小成交额（元）
    etf_dual_ma_confirm_days: int = 3  # 连续若干日 MA20>MA60

    # EtfMultiFactorStrategy：多因子横截面
    etf_mf_min_turnover_20d: float = 10_000_000.0  # 近20日日均成交额下限（元）
    etf_mf_max_5d_return_pct: float = 0.12  # 近5日涨幅上限
    etf_mf_max_drawdown_from_60d_high: float = 0.18  # 相对60日高点最大回撤
    etf_mf_max_results: int = 40  # 策略输出列表长度上限
    etf_mf_weight_liquidity: float = 0.35
    etf_mf_weight_mom20: float = 0.35
    etf_mf_weight_mom60: float = 0.30

    # EtfTrendFollowStrategy：趋势跟随
    etf_tf_min_turnover_20d: float = 8_000_000.0
    etf_tf_max_5d_return_pct: float = 0.15
    etf_tf_max_results: int = 40

    # 飞书合并日报（STRATEGY_WEBHOOK_DIGEST 可覆盖 digest webhook）
    feishu_digest_top_n: int = 5
    feishu_digest_max_per_strategy_display: int = 30
    feishu_push_mode_default: str = "auto"  # auto/morning/close/intraday
    feishu_morning_top_n: int = 5
    feishu_card_max_bytes_guard: int = 20_000
    feishu_send_min_interval_seconds: float = 0.25
    feishu_enable_intraday_warning: bool = True

    # 策略启停（默认关闭与主策略重叠较高的双均线策略）
    enable_ma_volume_strategy: bool = True
    enable_turtle_trade_strategy: bool = True
    enable_high_tight_flag_strategy: bool = True
    enable_etf_strong_pullback_strategy: bool = False
    enable_etf_uptrend_sharp_drop_strategy: bool = False
    enable_rps_breakout_strategy: bool = True
    enable_etf_dual_ma_trend_strategy: bool = False
    enable_etf_multi_factor_strategy: bool = True
    enable_etf_trend_follow_strategy: bool = True
    enable_adx_ma_regime_trend_strategy: bool = True
    enable_volume_contraction_breakout_strategy: bool = True
    enable_industry_relative_strength_rotation_strategy: bool = True
    enable_news_sentiment_breadth_strategy: bool = False
    enable_strong_trend_low_chase_strategy: bool = True
    enable_dual_momentum_rotation_strategy: bool = True
    enable_trend_stability_momentum_strategy: bool = True
    enable_low_vol_momentum_blend_strategy: bool = True

    # 策略权重（用于合并打分）
    strategy_weight_default: float = 1.0
    strategy_weight_ma_volume: float = 0.9
    strategy_weight_turtle: float = 1.1
    strategy_weight_flag: float = 0.8
    strategy_weight_shakeout: float = 0.5
    strategy_weight_limit_down: float = 0.5
    strategy_weight_rps: float = 1.0
    strategy_weight_etf_dual_ma: float = 0.7
    strategy_weight_etf_multi_factor: float = 1.3
    strategy_weight_etf_trend_follow: float = 1.2
    strategy_weight_adx_trend: float = 1.2
    strategy_weight_volume_contraction: float = 1.1
    strategy_weight_industry_rotation: float = 1.0
    strategy_weight_news_sentiment: float = 0.9
    strategy_weight_strong_trend_low_chase: float = 1.2
    strategy_weight_dual_momentum_rotation: float = 1.2
    strategy_weight_trend_stability_momentum: float = 1.2
    strategy_weight_low_vol_momentum_blend: float = 1.1

    # 市场状态门控（risk_on / risk_off）
    regime_benchmark_symbols: str = "510300,159915,510500"
    regime_ma_window: int = 20
    regime_strength_min_ratio: float = 0.55
    regime_breadth_min_ratio: float = 0.45
    regime_risk_on_trend_multiplier: float = 1.15
    regime_risk_on_reversal_multiplier: float = 0.85
    regime_risk_off_trend_multiplier: float = 0.85
    regime_risk_off_reversal_multiplier: float = 0.45
    regime_allow_reversal_when_risk_off: bool = False

    # RPS 突破增强参数
    rps_breakout_buffer: float = 0.0
    rps_min_turnover_20d: float = 8_000_000.0
    rps_max_atr_ratio: float = 0.08

    # 新增策略参数
    adx_period: int = 14
    adx_threshold: float = 18.0
    adx_atr_ratio_max: float = 0.09
    adx_min_turnover_20d: float = 8_000_000.0

    vcb_contraction_window: int = 15
    vcb_breakout_window: int = 40
    vcb_contraction_max_range_ratio: float = 0.10
    vcb_volume_breakout_multiplier: float = 1.8
    vcb_min_turnover_20d: float = 8_000_000.0
    vcb_max_5d_return_pct: float = 0.10
    vcb_min_close_position_ratio: float = 0.70

    industry_rotation_min_turnover_20d: float = 8_000_000.0
    industry_rotation_top_groups: int = 3
    industry_rotation_pick_per_group: int = 1
    industry_rotation_max_5d_return_pct: float = 0.10

    # 强趋势低追高隔夜策略
    stlc_min_turnover_20d: float = 10_000_000.0
    stlc_max_5d_return_pct: float = 0.10
    stlc_max_distance_from_ma20: float = 0.08
    stlc_max_upper_shadow_ratio: float = 0.30
    stlc_min_close_position_ratio: float = 0.70
    stlc_max_results: int = 30

    # 双动量轮动策略
    dmr_min_turnover_20d: float = 10_000_000.0
    dmr_max_5d_return_pct: float = 0.12
    dmr_max_results: int = 20

    # 趋势稳健动量策略
    tsm_lookback_days: int = 30
    tsm_min_turnover_20d: float = 10_000_000.0
    tsm_max_5d_return_pct: float = 0.12
    tsm_max_results: int = 20

    # 低波动动量融合策略
    lvmb_min_turnover_20d: float = 10_000_000.0
    lvmb_max_volatility_20d: float = 0.035
    lvmb_max_5d_return_pct: float = 0.10
    lvmb_max_results: int = 20

    # 消息面策略参数（通过 JSON 文件接入）
    news_signal_path: str = ""
    news_lookback_days: int = 3
    news_sentiment_threshold: float = 0.2
    news_heat_accel_threshold: float = 0.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # <--- 加上这一行！让 Pydantic 放行未定义的变量
    )

    @classmethod
    def settings_customise_sources(cls, settings_cls, **kwargs):  # type: ignore[override]
        """扩展配置源，支持从环境变量中扫描 STRATEGY_WEBHOOK_ 前缀的键。"""
        from pydantic_settings import EnvSettingsSource
        import os

        sources = super().settings_customise_sources(settings_cls, **kwargs)

        # 扫描环境变量，将 STRATEGY_WEBHOOK_<KEY> 收集到 strategy_webhooks
        prefix = "STRATEGY_WEBHOOK_"
        webhooks: dict[str, str] = {}
        for key, value in os.environ.items():
            if key.upper().startswith(prefix):
                strategy_key = key[len(prefix):].lower()
                webhooks[strategy_key] = value

        # 注入到初始化数据中（通过 init_kwargs source）
        if webhooks:
            original_init = kwargs.get("init_settings")
            # 直接在 env 层注入，通过 model_post_init 处理
            os.environ.setdefault("_STRATEGY_WEBHOOKS_PARSED", "1")
            # 存储解析结果供 model_validator 使用
            cls._parsed_strategy_webhooks = webhooks

        return sources

    def model_post_init(self, __context: object) -> None:
        """初始化后合并 STRATEGY_WEBHOOK_ 前缀的环境变量到 strategy_webhooks。"""
        import os

        prefix = "STRATEGY_WEBHOOK_"
        webhooks: dict[str, str] = dict(self.strategy_webhooks)
        for key, value in os.environ.items():
            if key.upper().startswith(prefix):
                strategy_key = key[len(prefix):].lower()
                webhooks[strategy_key] = value

        # 使用 object.__setattr__ 绕过 pydantic 的不可变保护
        object.__setattr__(self, "strategy_webhooks", webhooks)

    def get_webhook_url(self, webhook_key: str) -> str:
        """
        根据 webhook_key 返回对应的 Webhook URL。

        优先从 strategy_webhooks 查找，找不到则 fallback 到 feishu_webhook_url。

        Args:
            webhook_key: 策略标识，如 'ma_volume'、'breakout'。

        Returns:
            对应的 Webhook URL 字符串。
        """
        return self.strategy_webhooks.get(webhook_key.lower(), self.feishu_webhook_url)

    def is_strategy_enabled(self, key: str) -> bool:
        mapping = {
            "ma_volume": self.enable_ma_volume_strategy,
            "turtle": self.enable_turtle_trade_strategy,
            "flag": self.enable_high_tight_flag_strategy,
            "shakeout": self.enable_etf_strong_pullback_strategy,
            "limit_down": self.enable_etf_uptrend_sharp_drop_strategy,
            "rps": self.enable_rps_breakout_strategy,
            "etf_dual_ma": self.enable_etf_dual_ma_trend_strategy,
            "etf_multi_factor": self.enable_etf_multi_factor_strategy,
            "etf_trend_follow": self.enable_etf_trend_follow_strategy,
            "adx_trend": self.enable_adx_ma_regime_trend_strategy,
            "volume_contraction": self.enable_volume_contraction_breakout_strategy,
            "industry_rotation": self.enable_industry_relative_strength_rotation_strategy,
            "news_sentiment": self.enable_news_sentiment_breadth_strategy,
            "strong_trend_low_chase": self.enable_strong_trend_low_chase_strategy,
            "dual_momentum_rotation": self.enable_dual_momentum_rotation_strategy,
            "trend_stability": self.enable_trend_stability_momentum_strategy,
            "low_vol_momentum": self.enable_low_vol_momentum_blend_strategy,
        }
        return mapping.get(key.lower(), True)

    def get_strategy_weights(self) -> dict[str, float]:
        return {
            "MaVolumeStrategy": self.strategy_weight_ma_volume,
            "TurtleTradeStrategy": self.strategy_weight_turtle,
            "HighTightFlagStrategy": self.strategy_weight_flag,
            "EtfStrongPullbackStrategy": self.strategy_weight_shakeout,
            "EtfUptrendSharpDropStrategy": self.strategy_weight_limit_down,
            "RpsBreakoutStrategy": self.strategy_weight_rps,
            "EtfDualMaTrendStrategy": self.strategy_weight_etf_dual_ma,
            "EtfMultiFactorStrategy": self.strategy_weight_etf_multi_factor,
            "EtfTrendFollowStrategy": self.strategy_weight_etf_trend_follow,
            "AdxMaRegimeTrendStrategy": self.strategy_weight_adx_trend,
            "VolumeContractionBreakoutStrategy": self.strategy_weight_volume_contraction,
            "IndustryRelativeStrengthRotationStrategy": self.strategy_weight_industry_rotation,
            "NewsSentimentBreadthStrategy": self.strategy_weight_news_sentiment,
            "StrongTrendLowChaseStrategy": self.strategy_weight_strong_trend_low_chase,
            "DualMomentumRotationStrategy": self.strategy_weight_dual_momentum_rotation,
            "TrendStabilityMomentumStrategy": self.strategy_weight_trend_stability_momentum,
            "LowVolMomentumBlendStrategy": self.strategy_weight_low_vol_momentum_blend,
        }

    def get_strategy_groups(self) -> dict[str, str]:
        return {
            "MaVolumeStrategy": "trend",
            "TurtleTradeStrategy": "trend",
            "HighTightFlagStrategy": "trend",
            "EtfStrongPullbackStrategy": "reversal",
            "EtfUptrendSharpDropStrategy": "reversal",
            "RpsBreakoutStrategy": "trend",
            "EtfDualMaTrendStrategy": "trend",
            "EtfMultiFactorStrategy": "trend",
            "EtfTrendFollowStrategy": "trend",
            "AdxMaRegimeTrendStrategy": "trend",
            "VolumeContractionBreakoutStrategy": "trend",
            "IndustryRelativeStrengthRotationStrategy": "rotation",
            "NewsSentimentBreadthStrategy": "news",
            "StrongTrendLowChaseStrategy": "trend",
            "DualMomentumRotationStrategy": "rotation",
            "TrendStabilityMomentumStrategy": "trend",
            "LowVolMomentumBlendStrategy": "trend",
        }

    def get_regime_group_multipliers(self, regime: str) -> dict[str, float]:
        if regime == "risk_on":
            return {
                "trend": self.regime_risk_on_trend_multiplier,
                "reversal": self.regime_risk_on_reversal_multiplier,
                "rotation": 1.0,
                "news": 1.0,
            }
        return {
            "trend": self.regime_risk_off_trend_multiplier,
            "reversal": self.regime_risk_off_reversal_multiplier,
            "rotation": 1.0,
            "news": 1.0,
        }


_settings: Settings | None = None


def get_settings() -> Settings:
    """返回全局 Settings 单例。

    首次调用时从环境变量或 .env 文件加载配置。
    若必填字段（feishu_webhook_url）缺失，抛出 pydantic_core.ValidationError。

    Returns:
        Settings: 全局唯一的配置实例。

    Raises:
        pydantic_core.ValidationError: 当必填字段缺失或字段类型不匹配时抛出。
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
