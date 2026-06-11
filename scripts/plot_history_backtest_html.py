#!/usr/bin/env python3
"""从 results/html/历史回测-*.html 提取「画像汇总」月度累计收益，生成折线图到 results/images。

依赖：matplotlib（项目 venv 可执行 python -m pip install matplotlib）
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

_MONTH_RE = re.compile(r"<span><b>回测月份：</b>(\d{4}-\d{2})</span>")
_ROW_RE = re.compile(
    r"<td>(稳健版|激进版)</td><td class='num'>[^<]*</td>"
    r"<td class='num ret-(?:rise|fall)'>([+\-]?[\d.]+)%</td>"
)


def _parse_html(path: Path) -> tuple[str, dict[str, float]]:
    text = path.read_text(encoding="utf-8")
    mm = _MONTH_RE.search(text)
    if not mm:
        raise ValueError(f"未找到回测月份: {path}")
    profiles: dict[str, float] = {}
    for name, pct_s in _ROW_RE.findall(text):
        profiles[name] = float(pct_s)
    if "稳健版" not in profiles or "激进版" not in profiles:
        raise ValueError(f"画像汇总不完整: {path} -> {profiles}")
    return mm.group(1), profiles


def _collect_latest_per_month(html_dir: Path) -> list[tuple[str, dict[str, float], Path]]:
    """同一月份多份报告时，保留文件修改时间最新的一份。"""
    best: dict[str, tuple[float, dict[str, float], Path]] = {}
    for path in sorted(html_dir.glob("历史回测-*.html")):
        month, prof = _parse_html(path)
        mtime = path.stat().st_mtime
        cur = best.get(month)
        if cur is None or mtime >= cur[0]:
            best[month] = (mtime, prof, path)
    out = [(m, best[m][1], best[m][2]) for m in sorted(best.keys())]
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--html-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "results" / "html",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "results" / "images" / "monthly_backtest_profiles_returns.png",
    )
    parser.add_argument("--dpi", type=int, default=200)
    args = parser.parse_args()

    rows = _collect_latest_per_month(args.html_dir)
    if not rows:
        raise SystemExit(f"未找到历史回测 HTML：{args.html_dir / '历史回测-*.html'}")

    months = [r[0] for r in rows]
    conservative = [r[1]["稳健版"] for r in rows]
    aggressive = [r[1]["激进版"] for r in rows]

    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = [
        "WenQuanYi Micro Hei",
        "WenQuanYi Zen Hei",
        "Droid Sans Fallback",
    ]
    plt.rcParams["axes.unicode_minus"] = False

    fig, ax = plt.subplots(figsize=(11, 6.2), layout="constrained")
    x = range(len(months))
    ax.plot(x, conservative, "o-", color="#1565c0", linewidth=2.4, markersize=8, label="稳健版")
    ax.plot(x, aggressive, "s-", color="#c62828", linewidth=2.4, markersize=8, label="激进版")
    ax.axhline(0, color="#9e9e9e", linewidth=1, linestyle="--", zorder=0)
    def _zh_month(m: str) -> str:
        y, mo = m.split("-", 1)
        return f"{y}年{int(mo, 10)}月"

    ax.set_xticks(list(x), [_zh_month(m) for m in months])
    ax.set_ylabel("月度累计收益（%）")
    ax.set_title("Sequoia-X 月度回测 · 双画像累计收益")
    ax.grid(True, linestyle=":", alpha=0.75)
    ax.legend(loc="best", framealpha=0.95)

    fig.text(
        0.02,
        0.01,
        f"数据来源：results/html/历史回测-*.html（共 {len(rows)} 个月度报告，同月多份时取最新生成）",
        fontsize=9,
        color="#555",
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=args.dpi)
    plt.close(fig)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
