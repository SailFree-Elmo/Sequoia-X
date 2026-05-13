"""飞书通知模块：将 ETF 筛选结果通过 Webhook 推送至飞书群。"""

import json
import time
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
        self._last_send_ts: float = 0.0

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
        *,
        detailed: bool = False,
    ) -> str:
        """上一期保存的 Top 在次一交易日的表现，返回简洁摘要。"""
        header = "**昨日表现**"
        if not current_asof:
            return f"{header}：暂无（无行情数据）"
        if not prev_asof or not prev_codes:
            return f"{header}：暂无（缺少上一期记录）"
        d_next = engine.get_next_trading_date_after(prev_asof)
        if not d_next or d_next > current_asof:
            return f"{header}：暂无（上一期 {prev_asof} 的次日未入库）"
        rows, avg_o, avg_c = compute_pick_followthrough(
            engine, prev_asof, d_next, prev_codes, max_rows=top_n
        )
        valid_count = sum(1 for r in rows if r.pct_open_buy is not None or r.pct_close_buy is not None)
        if detailed:
            codes = sorted({r.code for r in rows})
            names = self._get_stock_names(codes) if codes else {}
            lines: list[str] = [
                f"{header}（{prev_asof}→{d_next}，样本 {valid_count}/{len(rows)}）",
                "",
            ]
            for i, r in enumerate(rows, start=1):
                nm = names.get(r.code, r.code)
                link = self._link_line(r.code, nm)
                lines.append(
                    f"{i}. {link} · 开盘买 {format_pct(r.pct_open_buy)} · 收盘买 {format_pct(r.pct_close_buy)}"
                )
            lines.append("")
            lines.append(f"开盘买均值 {format_pct(avg_o)} · 收盘买均值 {format_pct(avg_c)}")
            return "\n".join(lines)
        return (
            f"{header}（{prev_asof}→{d_next}，样本 {valid_count}/{len(rows)}）"
            f"\n开盘买均值 {format_pct(avg_o)} · 收盘买均值 {format_pct(avg_c)}"
        )

    def _build_digest_card(
        self,
        strategy_hits: dict[str, list[str]],
        turnover_by_symbol: dict[str, float] | None,
        *,
        yesterday_section: str | None = None,
        strategy_weights: dict[str, float] | None = None,
        strategy_groups: dict[str, str] | None = None,
        group_multipliers: dict[str, float] | None = None,
        push_mode: str = "intraday",
        asof_date: str | None = None,
        top_n_override: int | None = None,
        strategy_hits_alt: dict[str, list[str]] | None = None,
        strategy_weights_alt: dict[str, float] | None = None,
        strategy_groups_alt: dict[str, str] | None = None,
        group_multipliers_alt: dict[str, float] | None = None,
    ) -> dict:
        """综合推荐卡片：按 push_mode 生成 morning/close/intraday 三类视图。"""
        today = date.today().strftime("%Y-%m-%d")
        if top_n_override is not None:
            top_n = top_n_override
        elif push_mode == "morning":
            top_n = self.settings.feishu_morning_top_n
        else:
            top_n = self.settings.feishu_digest_top_n

        all_codes: set[str] = set()
        for lst in strategy_hits.values():
            all_codes.update(lst)
        if strategy_hits_alt:
            for lst in strategy_hits_alt.values():
                all_codes.update(lst)

        elements: list[dict] = []
        stale_data = bool(asof_date) and asof_date != today
        if asof_date:
            asof_text = f"**数据基准日**：{asof_date}"
            if stale_data:
                asof_text += "（非当日最新）"
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": asof_text}})
            elements.append({"tag": "hr"})

        if push_mode in {"close", "intraday"}:
            y_block = yesterday_section or "**昨日表现**：暂无"
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": y_block}})
            elements.append({"tag": "hr"})

        if not all_codes:
            title = "Sequoia-X ETF 收盘复盘" if push_mode == "close" else "Sequoia-X ETF 推荐"
            body = f"**{today}**\n今日无推荐。"
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})
            return {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": title},
                        "template": "blue",
                    },
                    "elements": elements,
                },
            }

        names = self._get_stock_names(sorted(all_codes))
        picks_main = rank_top_picks(
            strategy_hits,
            turnover_by_symbol,
            top_n=top_n,
            strategy_weights=strategy_weights,
            strategy_groups=strategy_groups,
            group_multipliers=group_multipliers,
        )
        lines_main: list[str] = [f"**稳健版 Top{top_n}**", ""]
        for i, p in enumerate(picks_main, start=1):
            nm = names.get(p.code, p.code)
            lines_main.append(
                f"{i}. {self._link_line(p.code, nm)} · {p.vote_count}票 / 加权{p.vote_score:.2f}"
            )
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines_main)}})

        if strategy_hits_alt:
            picks_alt = rank_top_picks(
                strategy_hits_alt,
                turnover_by_symbol,
                top_n=top_n,
                strategy_weights=strategy_weights_alt,
                strategy_groups=strategy_groups_alt,
                group_multipliers=group_multipliers_alt,
            )
            lines_alt: list[str] = [f"**激进版 Top{top_n}**", ""]
            for i, p in enumerate(picks_alt, start=1):
                nm = names.get(p.code, p.code)
                lines_alt.append(
                    f"{i}. {self._link_line(p.code, nm)} · {p.vote_count}票 / 加权{p.vote_score:.2f}"
                )
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines_alt)}})

        if push_mode == "intraday" and self.settings.feishu_enable_intraday_warning:
            warn = (
                f"**盘中提示**：当前为盘中快照，最新日K为 `{asof_date or '未知'}`，"
                "请勿将其视为当日收盘结论。"
            )
            elements.append({"tag": "hr"})
            elements.append({"tag": "div", "text": {"tag": "lark_md", "content": warn}})

        title_map = {
            "morning": "Sequoia-X ETF 盘前推荐",
            "close": "Sequoia-X ETF 收盘复盘",
            "intraday": "Sequoia-X ETF 盘中播报",
        }

        return {
            "msg_type": "interactive",
            "card": {
                "header": {
                    "title": {"tag": "plain_text", "content": title_map.get(push_mode, "Sequoia-X ETF 推荐")},
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
        strategy_weights: dict[str, float] | None = None,
        strategy_groups: dict[str, str] | None = None,
        group_multipliers: dict[str, float] | None = None,
        push_mode: str = "intraday",
        asof_date: str | None = None,
        strategy_hits_alt: dict[str, list[str]] | None = None,
        strategy_weights_alt: dict[str, float] | None = None,
        strategy_groups_alt: dict[str, str] | None = None,
        group_multipliers_alt: dict[str, float] | None = None,
    ) -> None:
        """合并多策略结果，**单次** POST 至 digest Webhook（未配置则用主 Webhook）。"""
        url = self.settings.get_webhook_url("digest")
        top_n = (
            self.settings.feishu_morning_top_n if push_mode == "morning" else self.settings.feishu_digest_top_n
        )
        payload = self._build_digest_card(
            strategy_hits,
            turnover_by_symbol,
            yesterday_section=yesterday_section,
            strategy_weights=strategy_weights,
            strategy_groups=strategy_groups,
            group_multipliers=group_multipliers,
            push_mode=push_mode,
            asof_date=asof_date,
            top_n_override=top_n,
            strategy_hits_alt=strategy_hits_alt,
            strategy_weights_alt=strategy_weights_alt,
            strategy_groups_alt=strategy_groups_alt,
            group_multipliers_alt=group_multipliers_alt,
        )
        max_bytes = int(self.settings.feishu_card_max_bytes_guard)
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        while len(encoded) > max_bytes and top_n > 1 and push_mode != "close":
            top_n -= 1
            payload = self._build_digest_card(
                strategy_hits,
                turnover_by_symbol,
                yesterday_section=yesterday_section,
                strategy_weights=strategy_weights,
                strategy_groups=strategy_groups,
                group_multipliers=group_multipliers,
                push_mode=push_mode,
                asof_date=asof_date,
                top_n_override=top_n,
                strategy_hits_alt=strategy_hits_alt,
                strategy_weights_alt=strategy_weights_alt,
                strategy_groups_alt=strategy_groups_alt,
                group_multipliers_alt=group_multipliers_alt,
            )
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if len(encoded) > max_bytes:
            logger.warning("飞书卡片体积仍超过限制：%d bytes", len(encoded))

        min_interval = float(self.settings.feishu_send_min_interval_seconds)
        elapsed = time.time() - self._last_send_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        try:
            resp = requests.post(
                url,
                data=encoded.decode("utf-8"),
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            self._last_send_ts = time.time()
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
