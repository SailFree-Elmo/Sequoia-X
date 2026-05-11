"""雪球代码前缀（沪市 ETF 等为 SH）。"""

import pytest

from sequoia_x.notify.feishu import FeishuNotifier


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("510300", "SH510300"),
        ("588000", "SH588000"),
        ("600519", "SH600519"),
        ("159919", "SZ159919"),
        ("430047", "BJ430047"),
    ],
)
def test_to_xueqiu_code(code: str, expected: str) -> None:
    assert FeishuNotifier._to_xueqiu_code(code) == expected


@pytest.mark.parametrize(
    ("code", "expected_prefix"),
    [
        ("510300", "sh"),
        ("159919", "sz"),
        ("600000", "sh"),
    ],
)
def test_bs_prefix(code: str, expected_prefix: str) -> None:
    assert FeishuNotifier._bs_prefix(code) == expected_prefix
