from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from dataeng.scale.scale_test import ScaleTestReport

_COLORS = {
    "surface": "#fcfcfb",
    "ink": "#0b0b0b",
    "ink_secondary": "#52514e",
    "muted": "#898781",
    "gridline": "#e1e0d9",
    "baseline": "#c3c2b7",
    "polars": "#2a78d6",
    "duckdb": "#eb6834",
}


def _fmt_seconds(value: float, _pos: int | None = None) -> str:
    if value >= 1:
        return f"{value:g}s"
    if value >= 0.001:
        return f"{value * 1000:g}ms"
    return f"{value * 1_000_000:g}µs"


def plot_scale_benchmark(report: ScaleTestReport, out_path: str) -> None:
    """Grouped bar chart, log-scale y-axis: the ~4 orders of magnitude
    between 'glob + filter' and 'direct partition path' would be
    unreadable on a linear axis — that gap *is* the finding."""
    strategies = ["full scan", "glob + filter", "direct partition path"]
    polars_times = [
        report.polars_full_scan.elapsed_seconds,
        report.polars_glob_filter.elapsed_seconds,
        report.polars_direct_partition.elapsed_seconds,
    ]
    duckdb_times = [
        report.duckdb_full_scan.elapsed_seconds,
        report.duckdb_glob_filter.elapsed_seconds,
        report.duckdb_direct_partition.elapsed_seconds,
    ]

    fig, ax = plt.subplots(figsize=(8, 5.5), facecolor=_COLORS["surface"])
    ax.set_facecolor(_COLORS["surface"])
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.spines["bottom"].set_visible(True)
    ax.spines["bottom"].set_color(_COLORS["baseline"])
    ax.tick_params(colors=_COLORS["muted"])
    ax.grid(axis="y", color=_COLORS["gridline"], linewidth=0.8, which="major")
    ax.set_axisbelow(True)

    x = range(len(strategies))
    width = 0.35
    ax.bar(
        [i - width / 2 for i in x], polars_times, width, color=_COLORS["polars"], label="polars"
    )
    ax.bar(
        [i + width / 2 for i in x], duckdb_times, width, color=_COLORS["duckdb"], label="duckdb"
    )

    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_seconds))
    ax.set_xticks(list(x))
    ax.set_xticklabels(strategies, color=_COLORS["ink_secondary"])
    ax.set_ylabel("query time (log scale)", color=_COLORS["ink_secondary"])
    ax.set_title(
        f"Partition access strategy vs. query time\n"
        f"({report.total_rows_in_dataset:,} rows across {report.total_files:,} files)",
        color=_COLORS["ink"],
        loc="left",
        fontsize=12,
        fontweight="bold",
    )
    legend = ax.legend(frameon=False)
    for text in legend.get_texts():
        text.set_color(_COLORS["ink_secondary"])

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, facecolor=_COLORS["surface"])
    plt.close(fig)


if __name__ == "__main__":
    from dataeng.scale.scale_test import run_partition_pruning_benchmark

    result = run_partition_pruning_benchmark("./dataeng_lake/scale_test")
    plot_scale_benchmark(result, "reports/scale_test_partition_pruning.png")
    print("chart written to reports/scale_test_partition_pruning.png")
