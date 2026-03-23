"""Archive card generator - reads metric_history.jsonl and produces matplotlib charts."""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

BASE_DIR = Path(__file__).parent.absolute()


def _read_history(experiment_dir: str) -> List[Dict[str, Any]]:
    history_file = os.path.join(experiment_dir, "metric_history.jsonl")
    entries: List[Dict[str, Any]] = []
    if not os.path.exists(history_file):
        return entries
    try:
        with open(history_file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
    except Exception:
        pass
    return entries


def get_metric_summary(
    exp_name: str, exp_dir: Optional[str] = None
) -> Dict[str, Dict[str, Any]]:
    if exp_dir is None:
        exp_dir = str(BASE_DIR / "experiments" / exp_name)
    entries = _read_history(exp_dir)
    if not entries:
        return {}
    skip_keys = {"epoch", "total_epochs", "timestamp", "phase", "experiment"}
    metric_names = set()
    for entry in entries:
        for k in entry:
            if k not in skip_keys and isinstance(entry[k], (int, float)):
                metric_names.add(k)
    summary: Dict[str, Dict[str, Any]] = {}
    for metric in sorted(metric_names):
        values = [
            entry[metric]
            for entry in entries
            if metric in entry and isinstance(entry[metric], (int, float))
        ]
        if values:
            summary[metric] = {
                "min": round(min(values), 4),
                "max": round(max(values), 4),
                "final": round(values[-1], 4),
                "mean": round(sum(values) / len(values), 4),
                "epochs_count": len(values),
            }
    return summary


def generate_archive_card(
    exp_name: str, exp_dir: Optional[str] = None, output_path: Optional[str] = None
) -> Optional[str]:
    if exp_dir is None:
        exp_dir = str(BASE_DIR / "experiments" / exp_name)
    entries = _read_history(exp_dir)
    if not entries:
        return None
    if output_path is None:
        output_path = os.path.join(exp_dir, "archive_card.png")
    skip_keys = {"epoch", "total_epochs", "timestamp", "phase", "experiment"}
    metric_names = []
    for entry in entries:
        for k in entry:
            if (
                k not in skip_keys
                and isinstance(entry[k], (int, float))
                and k not in metric_names
            ):
                metric_names.append(k)
    if not metric_names:
        return None
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None
    num_metrics = len(metric_names)
    fig, axes = plt.subplots(
        num_metrics, 1, figsize=(10, 3 * num_metrics), squeeze=False
    )
    fig.suptitle(exp_name, fontsize=14, fontweight="bold")
    for i, metric in enumerate(metric_names):
        ax = axes[i][0]
        epochs = [
            entry.get("epoch", j + 1)
            for j, entry in enumerate(entries)
            if metric in entry and isinstance(entry[metric], (int, float))
        ]
        values = [
            entry[metric]
            for entry in entries
            if metric in entry and isinstance(entry[metric], (int, float))
        ]
        if epochs and values:
            ax.plot(epochs, values, marker=".", markersize=3, linewidth=1)
            ax.set_ylabel(metric)
            ax.grid(True, alpha=0.3)
            if i == num_metrics - 1:
                ax.set_xlabel("Epoch")
    plt.tight_layout()
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    plt.savefig(output_path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    return output_path
