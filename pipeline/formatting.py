import re
from datetime import datetime
from typing import Any

from rich.text import Text


STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"


def format_time_ago(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds / 60)}m"
    elif seconds < 86400:
        return f"{int(seconds / 3600)}h"
    else:
        return f"{int(seconds / 86400)}d"


def make_bar(percent, width=15):
    filled = int(width * percent / 100)
    if percent >= 80:
        color = "red"
    elif percent >= 50:
        color = "yellow"
    else:
        color = "green"
    return f"[{color}]{'█' * filled}[/][dim]{'░' * (width - filled)}[/]"


def _parse_iso_ts(raw: Any) -> float | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw)).timestamp()
    except Exception:
        return None


_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _render_wait_progress(
    *, elapsed_sec: float | None, total_sec: float, width: int = 8
) -> str | None:
    if elapsed_sec is None:
        return None
    safe_elapsed = max(float(elapsed_sec), 0.0)
    frame = _SPINNER_FRAMES[int(safe_elapsed * 4) % len(_SPINNER_FRAMES)]
    mins, secs = divmod(int(safe_elapsed), 60)
    return f"{frame} loading {mins}m{secs:02d}s"


def normalize_status(raw_status: Any) -> str:
    status = str(raw_status or "").upper()
    if status in (STATUS_NEEDS_RERUN, STATUS_RUNNING, STATUS_COMPLETED):
        return status
    if status == "DONE":
        return STATUS_COMPLETED
    if status == "SKIPPED":
        return STATUS_COMPLETED
    if status in ("READY", "ERROR", "OOM"):
        return STATUS_NEEDS_RERUN
    return STATUS_NEEDS_RERUN


def normalize_initial_exp_page(page: int, total_pages: int) -> int:
    if total_pages <= 0:
        return 0
    try:
        page_num = int(page)
    except (TypeError, ValueError):
        return 0
    return max(0, min(total_pages - 1, page_num - 1))


def format_terminal_reason_text(terminal_reason: str) -> Text:
    raw = str(terminal_reason or "UNKNOWN").upper()
    normalized = raw.rstrip("*")
    styles = {
        "COMPLETED": "bold green",
        "RUNNING": "bold yellow",
        "FAILED_OOM": "bold red",
        "FAILED_SCRIPT_ERROR": "bold magenta",
        "FAILED_WITHOUT_METRIC": "bold bright_yellow",
        "QUEUED_RETRY": "bold cyan",
        "FROZEN": "bold blue",
    }
    return Text(raw, style=styles.get(normalized, "bold white"))
