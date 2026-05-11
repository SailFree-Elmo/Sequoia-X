"""飞书通知属性测试。"""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.notify.feishu import FeishuNotifier


def make_settings(webhook_url: str = "https://example.com/default") -> Settings:
    return Settings(
        db_path="data/test.db",
        start_date="2024-01-01",
        feishu_webhook_url=webhook_url,
    )


# Feature: sequoia-x-v2, Property 10: 飞书通知包含所有选股结果
@given(
    symbols=st.lists(
        st.text(min_size=6, max_size=6, alphabet="0123456789"),
        min_size=1, max_size=10, unique=True,
    )
)
@h_settings(max_examples=50)
def test_notification_contains_all_symbols(symbols: list[str]) -> None:
    """属性 10：send() 发出的请求体应包含所有 symbol。"""
    settings = make_settings()
    notifier = FeishuNotifier(settings)

    names = {s: f"n{s}" for s in symbols}
    with patch.object(FeishuNotifier, "_get_stock_names", return_value=names):
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock(status_code=200)
            mock_resp.json.return_value = {"code": 0}
            mock_resp.text = "{}"
            mock_post.return_value = mock_resp
            notifier.send(symbols=symbols, strategy_name="TestStrategy")

    call_args = mock_post.call_args
    body = json.loads(call_args.kwargs.get("data") or call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs["data"])
    card_text = json.dumps(body)
    for symbol in symbols:
        assert symbol in card_text


# Feature: sequoia-x-v2, Property 11: 飞书通知使用 ConfigManager 中的 Webhook URL
@given(
    webhook_url=st.from_regex(r"https://open\.feishu\.cn/open-apis/bot/v2/hook/[a-z0-9\-]{8,36}", fullmatch=True)
)
@h_settings(max_examples=50)
def test_notification_uses_config_url(webhook_url: str) -> None:
    """属性 11：send() 发出的 HTTP 请求目标 URL 应等于 settings.feishu_webhook_url。"""
    settings = make_settings(webhook_url=webhook_url)
    notifier = FeishuNotifier(settings)

    with patch.object(FeishuNotifier, "_get_stock_names", return_value={"000001": "测试"}):
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock(status_code=200)
            mock_resp.json.return_value = {"code": 0}
            mock_resp.text = "{}"
            mock_post.return_value = mock_resp
            notifier.send(symbols=["000001"], strategy_name="Test", webhook_key="default")

    called_url = mock_post.call_args.args[0] if mock_post.call_args.args else mock_post.call_args.kwargs.get("url")
    assert called_url == webhook_url


# Feature: sequoia-x-v2, Property 12: HTTP 失败时记录 ERROR 日志
@given(status_code=st.integers(min_value=400, max_value=599))
@h_settings(max_examples=50)
def test_http_failure_logs_error(status_code: int) -> None:
    """属性 12：非 200 响应时，send() 应记录 ERROR 级别日志，不抛出异常。"""
    import logging as _logging
    import sequoia_x.notify.feishu as feishu_module

    settings = make_settings()
    notifier = FeishuNotifier(settings)

    # feishu logger 设置了 propagate=False，需直接在其上挂 handler
    feishu_logger = _logging.getLogger(feishu_module.__name__)
    log_records: list[_logging.LogRecord] = []

    class _ListHandler(_logging.Handler):
        def emit(self, record: _logging.LogRecord) -> None:
            log_records.append(record)

    handler = _ListHandler(_logging.ERROR)
    feishu_logger.addHandler(handler)
    try:
        with patch.object(FeishuNotifier, "_get_stock_names", return_value={"000001": "测试"}):
            with patch("requests.post") as mock_post:
                mock_resp = MagicMock(status_code=status_code, text="error")
                mock_resp.json.return_value = {"code": -1}
                mock_post.return_value = mock_resp
                notifier.send(symbols=["000001"], strategy_name="Test")
    finally:
        feishu_logger.removeHandler(handler)

    assert any(r.levelno == _logging.ERROR for r in log_records)


def test_send_digest_posts_once_and_includes_union_codes() -> None:
    """合并日报只 POST 一次，且正文包含并集中的代码。"""
    settings = Settings(
        db_path="data/test.db",
        start_date="2024-01-01",
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/abcdefgh",
    )
    notifier = FeishuNotifier(settings)
    hits = {
        "MaVolumeStrategy": ["510300", "159919"],
        "TurtleTradeStrategy": ["510300"],
    }
    names = {"510300": "沪深300ETF", "159919": "深市ETF"}
    with patch.object(FeishuNotifier, "_get_stock_names", return_value=names):
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock(status_code=200)
            mock_resp.json.return_value = {"code": 0}
            mock_resp.text = "{}"
            mock_post.return_value = mock_resp
            notifier.send_digest(
                hits,
                turnover_by_symbol={"510300": 1e9, "159919": 1e6},
                yesterday_section="**昨日推荐表现**\n\ntest",
            )

    assert mock_post.call_count == 1
    called_url = mock_post.call_args.args[0]
    assert called_url == settings.feishu_webhook_url
    body = json.loads(mock_post.call_args.kwargs["data"])
    text_blob = json.dumps(body, ensure_ascii=False)
    assert "510300" in text_blob
    assert "159919" in text_blob
    assert "综合推荐" in text_blob or "Top10" in text_blob or "ETF 推荐" in text_blob
    assert "昨日推荐表现" in text_blob


def test_send_digest_uses_digest_webhook_when_configured() -> None:
    digest_url = "https://open.feishu.cn/open-apis/bot/v2/hook/digestonly12"
    settings = Settings(
        db_path="data/test.db",
        start_date="2024-01-01",
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/mainwebhook",
        strategy_webhooks={"digest": digest_url},
    )
    notifier = FeishuNotifier(settings)
    with patch.object(FeishuNotifier, "_get_stock_names", return_value={}):
        with patch("requests.post") as mock_post:
            mock_resp = MagicMock(status_code=200)
            mock_resp.json.return_value = {"code": 0}
            mock_post.return_value = mock_resp
            notifier.send_digest({})

    assert mock_post.call_args.args[0] == digest_url
