"""飞书通知模块：将 ETF 筛选结果通过 Webhook 推送至飞书群。"""

import json
from datetime import date

import requests

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.digest import rank_top_picks, union_symbol_count
from sequoia_x.notify.yesterday_perf import compute_pick_followthrough, format_pct

logger = get_logger(__name__)


class FeishuNotifier:
    """飞书 Webhook 推送器。

    日常合并日报使用 ``send_digest``（一次 POST，webhook_key ``digest``）。
    ``send`` 保留用于单策略场景或测试。
    """

    def __init__(self, settings: Settings) -> None:
        """
        初始化 FeishuNotifier。

        Args:
            settings: Settings 实例，提供 Webhook URL 配置。
        """
        self.settings = settings

    @staticmethod
    def _to_xueqiu_code(code: str) -> str:
        """将纯数字代码转为雪球格式：沪（5/6/9 开头）→SH，北交所（4/8）→BJ，其余→SZ。"""
        if code.startswith(("5", "6", "9")):
            return f"SH{code}"
        if code.startswith(("4", "8")):
            return f"BJ{code}"
        return f"SZ{code}"

    @staticmethod
    def _bs_prefix(code: str) -> str:
        """baostock 交易所前缀：沪市含 ETF（5 开头），深市含 159/15 等 ETF。"""
        return "sh" if code.startswith(("5", "6", "9")) else "sz"

    @staticmethod
    def _get_stock_names(symbols: list[str]) -> dict[str, str]:
        """通过 baostock 批量查询证券简称，返回 {code: name} 映射。"""
        import baostock as bs

        bs.login()
        mapping = {}
        for code in symbols:
            prefix = FeishuNotifier._bs_prefix(code)
            rs = bs.query_stock_basic(code=f"{prefix}.{code}")
            while rs.next():
                row = rs.get_row_data()
                mapping[code] = row[1]
        bs.logout()
        return mapping

    @staticmethod
    def _link_line(code: str, name: str) -> str:
        xq = FeishuNotifier._to_xueqiu_code(code)
        disp = name or xq
        return f"[{disp}](https://xueqiu.com/S/{xq}) `{code}`"

    def _build_card(self, symbols: list[str], strategy_name: str) -> dict:
        today = date.today().strftime("%Y-%m-%d")
        names = self._get_stock_names(symbols)

        links: list[str] = []
        for code in symbols:
            links.append(self._link_line(code, names.get(code, code)))

        symbol_text = "\n".join(links) if links else "（无筛选结果）"

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"📈 Sequoia-X ETF 播报 | {strategy_name}",
                    },
                    "template": "blue",
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": (
                                f"**日期：** {today}\n**策略：** {strategy_name}"
                                f"\n**ETF 数量：** {len(symbols)}"
                            ),
                        },
                    },
                    {"tag": "hr"},
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**列表：**\n{symbol_text}",
                        },
                    },
                ],
            },
        }

    def build_yesterday_perf_section(
        self,
        engine: DataEngine,
        prev_asof: str | None,
        prev_codes: list[str],
        current_asof: str | None,
        top_n: int,
    ) -> str:
        """上一期保存的 Top 在次一交易日的表现；无数据时也返回说明文案（飞书表格兼容性差，用列表）。"""
        header = "**昨日推荐表现**"
        if not current_asof:
            return f"{header}\n\n暂无：本地库中无行情日期（请先 backfill 并同步）。"
        if not prev_asof or not prev_codes:
            return (
                f"{header}\n\n暂无：尚无上一期保存的综合 Top（需至少成功跑完 1 次日常模式写入 "
                f"`digest_top_picks` 后，在下一交易日行情入库后再跑，即可显示对照）。"
            )
        d_next = engine.get_next_trading_date_after(prev_asof)
        if not d_next or d_next > current_asof:
            return (
                f"{header}\n\n暂无：推荐截止 **{prev_asof}** 的次一交易日 "
                f"**尚未出现在库中**（当前最新行情日为 **{current_asof}**），请待增量同步后再看。"
            )
        rows, avg_o, avg_c = compute_pick_followthrough(
            engine, prev_asof, d_next, prev_codes, max_rows=top_n
        )
        codes = sorted({r.code for r in rows})
        names = self._get_stock_names(codes) if codes else {}
        lines: list[str] = [
            f"{header}（推荐截止 **{prev_asof}**，观测日 **{d_next}**）",
            "",
            "开盘买：下一交易日开盘进、收盘出；收盘买：推荐日收盘进，下一交易日收盘出。",
            "",
        ]
        for i, r in enumerate(rows, start=1):
            nm = names.get(r.code, r.code)
            link = self._link_line(r.code, nm)
            lines.append(
                f"{i}. {link}  ·  开盘买 {format_pct(r.pct_open_buy)}  ·  收盘买 {format_pct(r.pct_close_buy)}"
            )
        lines.append("")
        lines.append(
            f"**等权均值**：开盘买 {format_pct(avg_o)} · 收盘买 {format_pct(avg_c)}"
        )
        return "\n".join(lines)

    def _build_digest_card(
        self,
        strategy_hits: dict[str, list[str]],
        turnover_by_symbol: dict[str, float] | None,
        *,
        yesterday_section: str | None = None,
    ) -> dict:
        """综合推荐 TopN；「昨日推荐表现」区块在上（默认可为占位说明）。"""
        today = date.today().strftime("%Y-%m-%d")
        top_n = self.settings.feishu_digest_top_n

        all_codes: set[str] = set()
        for lst in strategy_hits.values():
            all_codes.update(lst)

        elements: list[dict] = []

        y_block = yesterday_section or "**昨日推荐表现**\n\n暂无：未生成对照区块。"
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": y_block}})
        elements.append({"tag": "hr"})

        if not all_codes:
            body = f"**{today}**\n\n今日无命中。"
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})
            return {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": "Sequoia-X ETF 推荐"},
                        "template": "blue",
                    },
                    "elements": elements,
                },
            }

        names = self._get_stock_names(sorted(all_codes))
        picks = rank_top_picks(strategy_hits, turnover_by_symbol, top_n=top_n)

        lines: list[str] = [
            f"**{today}**  综合推荐 **Top{top_n}**（命中策略数 / Borda / 成交额）",
            "",
        ]
        for i, p in enumerate(picks, start=1):
            nm = names.get(p.code, p.code)
            lines.append(f"{i}. {self._link_line(p.code, nm)}  ·  **{p.vote_count}** 票")

        body = "\n".join(lines)
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": "Sequoia-X ETF 推荐"},
                    "template": "blue",
                },
                "elements": elements,
            },
        }

    def send_digest(
        self,
        strategy_hits: dict[str, list[str]],
        *,
        turnover_by_symbol: dict[str, float] | None = None,
        yesterday_section: str | None = None,
    ) -> None:
        """合并多策略结果，**单次** POST 至 digest Webhook（未配置则用主 Webhook）。"""
        url = self.settings.get_webhook_url("digest")
        payload = self._build_digest_card(
            strategy_hits, turnover_by_symbol, yesterday_section=yesterday_section
        )

        try:
            resp = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            resp_json = resp.json()

            if resp.status_code != 200 or resp_json.get("code") != 0:
                logger.error(
                    f"飞书合并日报推送失败 [digest] "
                    f"HTTP状态={resp.status_code} 飞书响应={resp.text}"
                )
            else:
                u = union_symbol_count(strategy_hits)
                logger.info(f"飞书合并日报推送成功 [digest]，并集 {u} 只代码")

        except requests.RequestException as exc:
            logger.error(f"飞书合并日报请求异常 [digest]：{exc}")

    def send(
        self,
        symbols: list[str],
        strategy_name: str,
        webhook_key: str = "default",
    ) -> None:
        """
        将 ETF 筛选结果格式化为飞书卡片消息并 POST 至对应 Webhook。

        根据 webhook_key 从 Settings 中查找专属 URL；
        若未配置，则 fallback 到 feishu_webhook_url。

        Args:
            symbols: 筛选结果代码列表（6 位数字）。
            strategy_name: 策略名称，用于卡片标题。
            webhook_key: 策略标识，用于路由到对应飞书机器人。

        Raises:
            不抛出异常，HTTP 失败时记录 ERROR 日志。
        """
        url = self.settings.get_webhook_url(webhook_key)
        payload = self._build_card(symbols, strategy_name)

        try:
            resp = requests.post(
                url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            resp_json = resp.json()

            if resp.status_code != 200 or resp_json.get("code") != 0:
                logger.error(
                    f"飞书推送失败 [{webhook_key}] "
                    f"HTTP状态={resp.status_code} 飞书响应={resp.text}"
                )
            else:
                logger.info(f"飞书推送成功 [{webhook_key}]，共 {len(symbols)} 只 ETF")

        except requests.RequestException as exc:
            logger.error(f"飞书推送请求异常 [{webhook_key}]：{exc}")
