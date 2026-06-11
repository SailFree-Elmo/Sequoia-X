"""配置管理属性测试。"""

import os
import pytest
from hypothesis import given, settings as h_settings, HealthCheck
from hypothesis import strategies as st
from pydantic import ValidationError


# Feature: sequoia-x-v2, Property 1: 环境变量覆盖配置默认值
@given(
    database_url=st.from_regex(
        r"postgresql://[a-zA-Z0-9_]+:[^@\x00]+@[a-zA-Z0-9\.\-]+:\d+/[a-zA-Z0-9_]+",
        fullmatch=True,
    )
)
@h_settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_env_overrides_default(database_url: str, monkeypatch) -> None:
    """属性 1：任意合法 database_url 通过环境变量设置后，Settings 实例应反映该值。"""
    import sequoia_x.core.config as cfg_module
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("FEISHU_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr(cfg_module, "_settings", None)
    from sequoia_x.core.config import Settings
    s = Settings()
    assert s.database_url == database_url


# Feature: sequoia-x-v2, Property 2: 缺失必填字段触发 ValidationError
def test_missing_required_field_raises() -> None:
    """属性 2：缺少 feishu_webhook_url 时，实例化 Settings 应抛出 ValidationError。"""
    import os
    from sequoia_x.core.config import Settings
    # 确保环境变量中没有该字段
    env_backup = os.environ.pop("FEISHU_WEBHOOK_URL", None)
    db_backup = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = "postgresql://user:pass@127.0.0.1:5433/test_db"
    try:
        with pytest.raises(ValidationError) as exc_info:
            Settings(_env_file=None)
        assert "feishu_webhook_url" in str(exc_info.value).lower()
    finally:
        if env_backup is not None:
            os.environ["FEISHU_WEBHOOK_URL"] = env_backup
        if db_backup is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = db_backup
