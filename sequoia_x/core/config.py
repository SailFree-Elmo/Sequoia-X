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
    feishu_digest_top_n: int = 10
    feishu_digest_max_per_strategy_display: int = 30

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
