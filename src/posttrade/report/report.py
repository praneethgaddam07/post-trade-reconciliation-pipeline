import json
from datetime import datetime, timedelta, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from posttrade.reconcile.reconciler import Reconciler, breaks_to_df
from posttrade.storage.book_store import BookStore

_COLORS = {
    "surface": "#fcfcfb",
    "ink": "#0b0b0b",
    "ink_secondary": "#52514e",
    "muted": "#898781",
    "gridline": "#e1e0d9",
    "baseline": "#c3c2b7",
    "throughput": "#2a78d6",
    "lag": "#eb6834",
}


def _fmt_rate(value: float, _pos: int | None = None) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    if value >= 1_000:
        return f"{value / 1_000:g}k"
    return f"{value:g}"


def plot_load_test(results_path: str, out_path: str) -> None:
    """Renders two single-axis panels (never a dual-axis chart) from the load
    harness's real step results: achieved throughput vs. target, and peak
    queue lag per step — the two signals that together show where the
    pipeline actually stops keeping up.

    Both axes on the throughput panel are log-scale: target rates span three
    orders of magnitude (2k -> 1.5M msg/s), so a linear y-axis would flatten
    every step below ~150k against the top of the range. The lag panel uses
    a symlog y-axis for the same reason, while still showing exact zeros
    (a linear-near-zero, log-further-out scale) since "lag was exactly zero"
    is itself meaningful — a log axis alone can't represent that."""
    with open(results_path) as f:
        steps = json.load(f)

    targets = [s["target_rate"] for s in steps]
    actual = [s["actual_publish_rate"] for s in steps]
    max_lag = [s["max_lag_during_step"] for s in steps]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 7.5), sharex=True, facecolor=_COLORS["surface"])
    for ax in (ax1, ax2):
        ax.set_facecolor(_COLORS["surface"])
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.spines["bottom"].set_visible(True)
        ax.spines["bottom"].set_color(_COLORS["baseline"])
        ax.tick_params(colors=_COLORS["muted"])
        ax.grid(axis="y", color=_COLORS["gridline"], linewidth=0.8, which="major")
        ax.set_axisbelow(True)

    ax1.plot(targets, targets, linestyle="--", color=_COLORS["muted"], linewidth=1.2, label="target rate (ideal)")
    ax1.plot(targets, actual, marker="o", color=_COLORS["throughput"], linewidth=2, label="actual publish rate")
    ax1.set_ylabel("messages / sec (log scale)", color=_COLORS["ink_secondary"])
    ax1.set_title(
        "Achieved publish throughput vs. target rate", color=_COLORS["ink"], loc="left", fontsize=12, fontweight="bold"
    )
    legend = ax1.legend(frameon=False)
    for text in legend.get_texts():
        text.set_color(_COLORS["ink_secondary"])

    ax2.plot(targets, max_lag, marker="o", color=_COLORS["lag"], linewidth=2)
    ax2.set_ylabel("peak consumer lag, messages\n(symlog scale)", color=_COLORS["ink_secondary"])
    ax2.set_xlabel("target publish rate (msg/s, log scale)", color=_COLORS["ink_secondary"])
    ax2.set_title(
        "Peak queue lag during each load step", color=_COLORS["ink"], loc="left", fontsize=12, fontweight="bold"
    )

    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax2.set_xscale("log")
    ax2.set_yscale("symlog", linthresh=100)

    for ax in (ax1, ax2):
        ax.xaxis.set_major_locator(mticker.LogLocator(base=10))
        ax.xaxis.set_minor_locator(mticker.LogLocator(base=10, subs=(2, 5)))
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_rate))
        ax.xaxis.set_minor_formatter(mticker.FuncFormatter(_fmt_rate))
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_rate))
    ax2.tick_params(axis="x", which="minor", labelsize=8, labelcolor=_COLORS["muted"])
    ax2.tick_params(axis="x", which="major", labelsize=9)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, facecolor=_COLORS["surface"])
    plt.close(fig)


def generate_break_report(symbols: list[str], hours_back: int = 6, minutes_forward: int = 30) -> str:
    """Runs the reconciler against whatever is currently in storage and
    formats the result as markdown, including a comparison against the
    Phase 5 ground truth log if any planted anomalies are present."""
    reconciler = Reconciler()
    book_store = BookStore()
    start = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    end = datetime.now(timezone.utc) + timedelta(minutes=minutes_forward)

    try:
        breaks = reconciler.reconcile_all(symbols, start, end)
        df = breaks_to_df(breaks)
        ground_truth = book_store.read_planted_anomalies_df()
    finally:
        reconciler.close()
        book_store.close()

    lines = ["# Break Report", ""]
    lines.append(f"Generated {datetime.now(timezone.utc).isoformat()} — symbols: {', '.join(symbols)}")
    lines.append("")
    lines.append(f"**Total breaks found: {len(breaks)}**")
    lines.append("")

    if not df.empty:
        lines.append("| Break type | Count |")
        lines.append("|---|---|")
        for break_type, count in df["break_type"].value_counts().items():
            lines.append(f"| {break_type} | {count} |")
        lines.append("")

    if not ground_truth.empty:
        lines.append("## Measured precision / recall against planted ground truth")
        lines.append("")
        lines.append("| Anomaly type | Planted | Found (matching) | Precision | Recall |")
        lines.append("|---|---|---|---|---|")
        for anomaly_type in ["unmatched_fill", "position_drift"]:
            planted_ids = set(ground_truth[ground_truth["anomaly_type"] == anomaly_type]["related_fill_id"].dropna())
            found_ids = set(
                df[df["break_type"] == anomaly_type]["related_fill_id"].dropna() if not df.empty else []
            )
            tp = len(planted_ids & found_ids)
            fn = len(planted_ids - found_ids)
            fp = len(found_ids - planted_ids)
            precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
            recall = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
            lines.append(
                f"| {anomaly_type} | {len(planted_ids)} | {tp} | {precision:.3f} | {recall:.3f} |"
            )
        planted_gap_events = len(ground_truth[ground_truth["anomaly_type"] == "sequence_gap"])
        found_gap_events = len(df[df["break_type"] == "sequence_gap"]) if not df.empty else 0
        lines.append(
            f"| sequence_gap | {planted_gap_events} (events) | {found_gap_events} (contiguous runs found) | — | — |"
        )
        lines.append("")
        lines.append(
            "sequence_gap is scored by structural comparison, not a shared ID: adjacent or "
            "overlapping planted gaps can merge into one contiguous missing run in the underlying "
            "data, so found-runs can be slightly below planted-events even at perfect detection."
        )
        lines.append("")

    if not df.empty:
        lines.append("## Sample breaks")
        lines.append("")
        for _, row in df.sort_values("detected_at").head(15).iterrows():
            lines.append(f"- `{row['break_type']}` [{row['symbol']}] {row['detected_at']} — {row['detail']}")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    report_md = generate_break_report(["BTC-USD", "ETH-USD"])
    with open("reports/break_report.md", "w") as f:
        f.write(report_md)
    print(report_md)

    try:
        plot_load_test("reports/load_test_results.json", "reports/load_test_throughput_lag.png")
        print("\nchart written to reports/load_test_throughput_lag.png")
    except FileNotFoundError:
        print("\nno load_test_results.json found -- run the load harness first")
