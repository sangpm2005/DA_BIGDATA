"""Render report figures from Spark's bounded dashboard aggregate, never raw CSV."""
import argparse
import json
from pathlib import Path
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dashboard.server import Store, PLATFORMS

COLORS = {"Shopee": "#ed7657", "Lazada": "#7a63d7", "Sendo": "#c9963f", "Tiki": "#2b9cae"}


def number(value):
    return f"{int(value):,}".replace(",", ".")


def decorate(ax):
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#dce5e9")
    ax.tick_params(colors="#536b78", length=0, pad=8)
    ax.set_axisbelow(True)


def save(fig, folder, name, metadata, note):
    fig.text(.035, .048, note, color="#5c7480", fontsize=9)
    fig.text(.035, .017,
        f"Google Play · 06/2013–08/2022 · Spark/Parquet HDFS · snapshot {metadata['generation'][:12]}",
        color="#82949e", fontsize=8)
    for extension in ["png", "svg"]:
        fig.savefig(folder / f"{name}.{extension}", dpi=200, facecolor="white")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outputs", type=Path, default=Path(__file__).resolve().parents[1] / "outputs")
    args = parser.parse_args()
    store = Store(args.outputs)
    metadata, _ = store.snapshot()
    data = store.overview({})
    folder = args.outputs / "report_charts"
    folder.mkdir(exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11,
        "axes.labelcolor": "#536b78", "text.color": "#173c49", "svg.fonttype": "none"})

    # Equal zero baseline and the same denominator definition for every platform.
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.subplots_adjust(left=.14, right=.84, top=.77, bottom=.23)
    platforms = sorted(data["platforms"], key=lambda p: p["rate"], reverse=True)
    positions = np.arange(len(platforms))
    ax.barh(positions, [p["rate"] for p in platforms], color=[COLORS[p["platform"]] for p in platforms], height=.54)
    ax.set_yticks(positions, [p["platform"] for p in platforms])
    ax.invert_yaxis()
    ax.set_xlim(0, .34)
    ax.xaxis.set_major_formatter(PercentFormatter(1))
    ax.grid(axis="x", alpha=.18)
    for i, p in enumerate(platforms):
        ax.text(p["rate"] + .007, i, f"{p['rate']*100:.2f}%".replace(".", ","), va="center", weight="bold")
        ax.text(.34, i + .36, f"{number(p['low'])} / {number(p['reviews'])} review", va="center", ha="right", fontsize=9, color="#758b96")
    ax.set_xlabel("Review 1–2 sao / tổng review đã làm sạch của từng sàn")
    decorate(ax)
    fig.suptitle("Tỷ lệ đánh giá thấp trong tập phản hồi Google Play", x=.035, ha="left", y=.93, fontsize=17, weight="bold")
    fig.text(.035, .855, f"{number(data['stats']['reviews'])} review · {number(data['stats']['low'])} review 1–2 sao", fontsize=11, color="#6b8390")
    save(fig, folder, "01_platform_low_rating", metadata,
        "Tỷ lệ trong dataset lịch sử; không đại diện mức hài lòng của toàn bộ khách hàng Việt Nam.")

    annual = {}
    for row in data["series"]:
        key = (row["platform"], int(row["month"][:4]))
        annual.setdefault(key, [0, 0])
        annual[key][0] += row["reviews"]
        annual[key][1] += row["low"]
    years = np.arange(2013, 2023)
    fig, axes = plt.subplots(2, 1, figsize=(11, 8), sharex=True, gridspec_kw={"height_ratios": [1.5, 1]})
    fig.subplots_adjust(left=.10, right=.96, top=.82, bottom=.17, hspace=.22)
    for platform in PLATFORMS:
        totals = [annual.get((platform, int(y)), [0, 0]) for y in years]
        axes[0].plot(years, [low / total if total else np.nan for total, low in totals],
            color=COLORS[platform], marker="o", linewidth=2, label=platform)
        axes[1].plot(years, [total if total else np.nan for total, _ in totals], color=COLORS[platform], marker="o", linewidth=1.7)
    axes[0].set_ylim(0, .65)
    axes[0].yaxis.set_major_formatter(PercentFormatter(1))
    axes[0].set_ylabel("Tỷ lệ 1–2 sao")
    axes[1].set_ylabel("Số review")
    axes[1].set_ylim(bottom=0)
    axes[1].ticklabel_format(axis="y", style="plain")
    axes[1].set_xticks(years)
    for ax in axes:
        decorate(ax)
        ax.grid(alpha=.18)
    axes[0].legend(frameon=False, ncol=4, loc="upper left", bbox_to_anchor=(-.01, 1.2))
    fig.suptitle("Xu hướng sao thấp và quy mô phản hồi theo năm", x=.035, ha="left", y=.95, fontsize=17, weight="bold")
    save(fig, folder, "02_annual_trend_and_volume", metadata,
        "Tỷ lệ tính từ tổng số đếm từng năm. Năm 2013 và 2022 chỉ có một phần năm; điểm ít review biến động mạnh.")

    aspects = sorted(data["aspects"], key=lambda a: a["mentions"], reverse=True)
    platforms = data["platforms"]
    matrix = np.array([[p["aspects"][a["code"]] / p["low"] for p in platforms] for a in aspects])
    fig, ax = plt.subplots(figsize=(11, 7.5))
    fig.subplots_adjust(left=.25, right=.86, top=.77, bottom=.20)
    image = ax.imshow(matrix, cmap="BuGn", vmin=0, vmax=.3, aspect="auto")
    ax.set_yticks(range(len(aspects)), [a["label"] for a in aspects])
    ax.set_xticks(range(len(platforms)), [p["platform"] + "\n" + number(p["low"]) + " sao thấp" for p in platforms])
    ax.xaxis.tick_top()
    for i, a in enumerate(aspects):
        for j, p in enumerate(platforms):
            ax.text(j, i, f"{matrix[i,j]*100:.2f}%\n({number(p['aspects'][a['code']])} review)".replace(".", ",", 1),
                ha="center", va="center", fontsize=10, color="white" if matrix[i,j] > .21 else "#214d49")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(length=0, pad=10)
    colorbar = fig.colorbar(image, ax=ax, fraction=.038, pad=.04)
    colorbar.ax.yaxis.set_major_formatter(PercentFormatter(1))
    colorbar.outline.set_visible(False)
    fig.suptitle("Khía cạnh được đề cập trong review 1–2 sao", x=.035, ha="left", y=.95, fontsize=17, weight="bold")
    fig.text(.035, .895, "Mỗi ô: số review sao thấp nhắc khía cạnh / tổng review sao thấp cùng sàn", color="#6b8390", fontsize=10)
    save(fig, folder, "03_aspect_platform_heatmap", metadata,
        "Một review có thể nhắc nhiều khía cạnh. Nhận diện bằng luật; đề cập chưa xác nhận cảm xúc tiêu cực tại khía cạnh.")

    (folder / "chart_manifest.json").write_text(json.dumps({
        "generation": metadata["generation"], "source_sha256": metadata.get("source_sha256"),
        "charts": sorted(p.name for p in folder.glob("*.png")), "scope": "all platforms, all available months, spam candidates retained",
        "reviews": data["stats"]["reviews"], "low_rating_reviews": data["stats"]["low"]}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved three PNG/SVG figure pairs to {folder}")


if __name__ == "__main__":
    main()
