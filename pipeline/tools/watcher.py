#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Auto-Wake Watcher - 實驗批次完成監控與 Agent 喚醒系統
Version: 1.0.0

功能:
- 監控 experiments.json 中的批次完成狀態
- 當批次全部完成（DONE/ERROR/TRUEOOM）時，透過 tmux send-keys 喚醒 AI Agent
- 支援延遲通知（避免 Agent 正在忙碌時打斷）
- 原子化 JSON 讀寫操作
- 分層記憶系統支援

使用方式:
    python watcher.py                    # 啟動監控循環
    python watcher.py --once             # 單次檢查
    python watcher.py --interval 60      # 設定檢查間隔（秒）
    python watcher.py --delay 300        # 設定延遲通知時間（秒）

tmux Session:
    建議在獨立的 "watcher" session 中運行
    tmux new-session -d -s watcher "python Phase3/tools/watcher.py"
"""

import os
import sys
import json
import time
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
import argparse
import signal
import fcntl
import socket

# =============================================================================
# 路徑設定
# =============================================================================

BASE_DIR = Path(__file__).parent.parent.absolute()
PROJECT_ROOT = BASE_DIR.parent.absolute()

EXPERIMENTS_FILE = BASE_DIR / "experiments.json"
RESULTS_DIR = BASE_DIR / "results_db"
LOGS_DIR = BASE_DIR / "logs"

MEMORY_DIR = PROJECT_ROOT / "Memory"
MEMORY_SHORT = MEMORY_DIR / "Short.md"
MEMORY_LONG = MEMORY_DIR / "Long.md"

ERROR_PATTERNS_FILE = BASE_DIR / "tools" / "error_patterns.json"
WATCHER_STATE_FILE = BASE_DIR / "tools" / ".watcher_state.json"
WATCHER_QUEUE_FILE = BASE_DIR / "tools" / ".watcher_queue.json"
WATCHER_LOG_FILE = BASE_DIR / "tools" / "watcher.log"

# Make Phase3 modules importable when running as a script.
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from cli_shared import add_common_args, setup_logging, emit_result

try:
    from registry_io import load_registry as load_phase3_registry
    from registry_io import registry_lock as phase3_registry_lock
except Exception:
    load_phase3_registry = None
    phase3_registry_lock = None

# =============================================================================
# 安全限制 (可配置)
# =============================================================================

MAX_EXPERIMENTS_PER_BATCH = 6  # 每批次最多 N 個實驗
MAX_CONSECUTIVE_FAILURES = 3  # 連續失敗 M 次後停止
NOTIFICATION_DELAY_SECONDS = 180  # 延遲通知時間（3分鐘）
CHECK_INTERVAL_SECONDS = 30  # 檢查間隔
WATCHER_SINGLETON_STALE_SEC = 600  # watcher.lock 超過此秒數視為 stale

# 終態定義
TERMINAL_STATES = {"DONE", "COMPLETED", "ERROR", "OOM"}

# =============================================================================
# 原子化 JSON 操作
# =============================================================================


def atomic_read_json(filepath: Path) -> Optional[Dict]:
    """原子化讀取 JSON 文件（帶文件鎖）"""
    if not filepath.exists():
        return None

    try:
        with open(filepath, "r") as f:
            # 獲取共享鎖（允許其他讀取）
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                return json.load(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except (json.JSONDecodeError, IOError) as e:
        log(f"[ERROR] 讀取 {filepath} 失敗: {e}")
        return None


def atomic_write_json(filepath: Path, data: Dict) -> bool:
    """原子化寫入 JSON 文件（臨時文件 + 重命名）"""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    try:
        # 寫入臨時文件
        fd, tmp_path = tempfile.mkstemp(
            dir=filepath.parent, prefix=f".{filepath.stem}_", suffix=".tmp"
        )

        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # 原子重命名
            os.replace(tmp_path, filepath)
            return True

        except Exception as e:
            # 清理臨時文件
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise e

    except Exception as e:
        log(f"[ERROR] 寫入 {filepath} 失敗: {e}")
        return False


def locked_read_json(filepath: Path) -> Optional[Dict]:
    """讀取 JSON（跨進程 sidecar lock 優先，fallback 到 flock）。"""
    if phase3_registry_lock is None:
        return atomic_read_json(filepath)
    if not filepath.exists():
        return None
    try:
        with phase3_registry_lock(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        log(f"[ERROR] 讀取 {filepath} 失敗: {e}")
        return None


def locked_write_json(filepath: Path, data: Dict) -> bool:
    """寫入 JSON（跨進程 sidecar lock 優先，fallback 到 atomic_write_json）。"""
    if phase3_registry_lock is None:
        return atomic_write_json(filepath, data)
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with phase3_registry_lock(filepath):
            fd, tmp_path = tempfile.mkstemp(
                dir=str(filepath.parent),
                prefix=f".{filepath.stem}_",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                    except Exception:
                        pass
                os.replace(tmp_path, filepath)
                return True
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
    except Exception as e:
        log(f"[ERROR] 寫入 {filepath} 失敗: {e}")
        return False


def read_experiments_registry() -> Optional[Dict]:
    """讀取 Phase3 experiments registry（統一與 runner 相同的 IO 行為）。"""
    try:
        if load_phase3_registry is not None:
            return load_phase3_registry(EXPERIMENTS_FILE)
    except Exception as e:
        log(f"[WARN] load_registry 失敗，fallback atomic read: {e}")
    return atomic_read_json(EXPERIMENTS_FILE)


# =============================================================================
# 日誌
# =============================================================================


def log(message: str, also_print: bool = True):
    """寫入日誌"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"

    if also_print:
        print(line)

    try:
        with open(WATCHER_LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass


# =============================================================================
# 批次狀態分析
# =============================================================================


@dataclass
class BatchStatus:
    """批次狀態數據類"""

    batch_id: str
    total: int = 0
    done: int = 0
    error: int = 0
    running: int = 0
    ready: int = 0
    experiments: List[Dict] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        """批次是否已全部完成"""
        return self.running == 0 and self.ready == 0 and self.total > 0

    @property
    def success_rate(self) -> float:
        """成功率"""
        return self.done / self.total if self.total > 0 else 0


def analyze_batch_status(experiments: List[Dict]) -> Dict[str, BatchStatus]:
    """
    分析所有批次的狀態
    Returns: {batch_id: BatchStatus}
    """
    batches: Dict[str, BatchStatus] = {}

    for exp in experiments:
        batch_id = exp.get("batch_id", "unknown")

        if batch_id not in batches:
            batches[batch_id] = BatchStatus(batch_id=batch_id)

        batch = batches[batch_id]
        batch.total += 1
        batch.experiments.append(exp)

        status = str(exp.get("status", "READY") or "READY").upper()
        error_info = exp.get("error_info") or {}
        is_true_oom = error_info.get("is_true_oom", False)
        retry_count = exp.get("retry_count", 0)

        if status in ("DONE", "COMPLETED"):
            batch.done += 1
        elif status in ("ERROR", "OOM"):
            # TRUE OOM 或已重試 2 次 = 終態
            if is_true_oom or retry_count >= 2:
                batch.error += 1
            else:
                batch.ready += 1  # 會被 runner 重試
        elif status == "RUNNING":
            batch.running += 1
        else:
            batch.ready += 1

    return batches


# =============================================================================
# 錯誤分析
# =============================================================================


def classify_error(error_info: Dict) -> Tuple[str, str, str]:
    """
    分類錯誤類型並提供修復建議
    Returns: (category, root_cause, fix_suggestion)
    """
    error_type = error_info.get("type", "UNKNOWN")
    message = error_info.get("message", "")
    peak_mem = error_info.get("peak_memory_mb", 0)
    is_true_oom = error_info.get("is_true_oom", False)

    # OOM 類
    if error_type == "OOM" or "CUDA out of memory" in message:
        if is_true_oom or peak_mem > 22000:
            return (
                "TRUEOOM",
                f"GPU 記憶體不足 (Peak: {peak_mem}MB)",
                "需要架構改動: 減少 layers/hidden_dim 或使用 gradient checkpointing",
            )
        else:
            return (
                "SOFTOOM",
                f"臨時 OOM (Peak: {peak_mem}MB)",
                "嘗試降低 batch_size 或 num_neighbors",
            )

    # 代碼錯誤
    if error_type == "SCRIPT_ERROR":
        if "shape mismatch" in message.lower() or "size mismatch" in message.lower():
            return ("SHAPE_ERROR", "張量維度不匹配", "檢查特徵維度和模型輸入輸出")
        elif "keyerror" in message.lower():
            return ("KEY_ERROR", "字典鍵不存在", "檢查特徵名稱或配置項是否正確")
        elif "indexerror" in message.lower():
            return (
                "INDEX_ERROR",
                "索引越界",
                "檢查 batch slicing 邏輯 (常見: batch.y[:batch.batch_size])",
            )
        elif "attributeerror" in message.lower():
            return ("ATTR_ERROR", "屬性不存在", "檢查物件類型和方法調用")
        else:
            return (
                "SCRIPT_ERROR",
                "腳本執行錯誤",
                "查看完整 traceback: Phase3/logs/{exp_name}.err",
            )

    # 進程異常
    if error_type == "ZOMBIE":
        return (
            "ZOMBIE",
            "進程意外終止",
            "可能是系統 OOM killer 或外部中斷，檢查 dmesg",
        )

    return ("UNKNOWN", "未知錯誤", "手動檢查日誌")


def update_error_patterns(
    exp_name: str, error_info: Dict, fix_applied: Optional[str] = None
):
    """更新錯誤模式庫"""
    patterns = locked_read_json(ERROR_PATTERNS_FILE) or {"patterns": [], "stats": {}}

    category, root_cause, suggestion = classify_error(error_info)

    pattern = {
        "experiment": exp_name,
        "timestamp": datetime.now().isoformat(),
        "category": category,
        "root_cause": root_cause,
        "suggestion": suggestion,
        "raw_error": error_info.get("message", "")[:500],
        "fix_applied": fix_applied,
    }

    patterns["patterns"].append(pattern)

    # 更新統計
    stats = patterns.get("stats", {})
    stats[category] = stats.get(category, 0) + 1
    patterns["stats"] = stats

    # 保留最近 100 條
    if len(patterns["patterns"]) > 100:
        patterns["patterns"] = patterns["patterns"][-100:]

    locked_write_json(ERROR_PATTERNS_FILE, patterns)


def update_error_patterns_batch(entries: List[Tuple[str, Dict]]):
    """批量更新錯誤模式庫，避免多次 I/O 造成競態與延遲。"""
    if not entries:
        return

    patterns = locked_read_json(ERROR_PATTERNS_FILE) or {"patterns": [], "stats": {}}
    stats = patterns.get("stats", {})

    for exp_name, error_info in entries:
        category, root_cause, suggestion = classify_error(error_info or {})
        pattern = {
            "experiment": exp_name,
            "timestamp": datetime.now().isoformat(),
            "category": category,
            "root_cause": root_cause,
            "suggestion": suggestion,
            "raw_error": (error_info or {}).get("message", "")[:500],
            "fix_applied": None,
        }
        patterns["patterns"].append(pattern)
        stats[category] = stats.get(category, 0) + 1

    patterns["stats"] = stats

    if len(patterns["patterns"]) > 100:
        patterns["patterns"] = patterns["patterns"][-100:]

    locked_write_json(ERROR_PATTERNS_FILE, patterns)


# =============================================================================
# Agent 喚醒
# =============================================================================


def generate_wake_prompt(batch_id: str, batch: BatchStatus) -> str:
    """生成簡潔的喚醒提示"""
    success_count = batch.done
    fail_count = batch.error
    total = batch.total

    # 簡潔版本
    prompt = f'[AUTO-WAKE] Batch "{batch_id}" 完成 ({success_count}/{total} 成功)'

    if total > MAX_EXPERIMENTS_PER_BATCH:
        prompt += f" | ⚠️ batch 過大({total}>{MAX_EXPERIMENTS_PER_BATCH})，請手動 review"
        return prompt

    if fail_count > 0:
        failed_names = [
            e["name"] for e in batch.experiments if e.get("status") in ("ERROR", "OOM")
        ]
        prompt += f" | 失敗: {', '.join(failed_names[:3])}"
        if len(failed_names) > 3:
            prompt += f"... (+{len(failed_names) - 3})"

    return prompt


def wake_coder_session(prompt: str) -> bool:
    """Hybrid 喚醒：先顯示訊息；僅在 exp_runner 前景為 opencode 時才注入 stdin。"""
    # 檢查 exp_runner session 是否存在
    check_cmd = ["tmux", "has-session", "-t", "exp_runner"]
    result = subprocess.run(check_cmd, capture_output=True)

    if result.returncode != 0:
        log("[WARN] exp_runner session 不存在，跳過喚醒")
        return False

    try:
        # Always show message without injecting input.
        subprocess.run(
            ["tmux", "display-message", "-t", "exp_runner", prompt],
            capture_output=True,
            text=True,
            timeout=5,
        )

        # Determine active pane current command.
        pane_cmd = None
        panes = subprocess.run(
            [
                "tmux",
                "list-panes",
                "-t",
                "exp_runner",
                "-F",
                "#{pane_active} #{pane_current_command}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if panes.returncode == 0:
            for line in panes.stdout.splitlines():
                if line.startswith("1 "):
                    pane_cmd = line.split(" ", 1)[1].strip()
                    break

        if not pane_cmd or "opencode" not in pane_cmd:
            log(f"[INFO] 只顯示通知，不注入 stdin (pane_cmd={pane_cmd or '-'})")
            return True

        # Inject only when OpenCode is in foreground.
        send1 = subprocess.run(
            ["tmux", "send-keys", "-t", "exp_runner", "-l", prompt],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if send1.returncode != 0:
            log(f"[ERROR] tmux send-keys 失敗: {send1.stderr}")
            return False
        send2 = subprocess.run(
            ["tmux", "send-keys", "-t", "exp_runner", "C-m"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if send2.returncode == 0:
            log("[OK] 已喚醒 exp_runner session")
            return True
        log(f"[ERROR] tmux send-keys Enter 失敗: {send2.stderr}")
        return False
    except subprocess.TimeoutExpired:
        log("[ERROR] tmux 命令超時")
        return False
    except Exception as e:
        log(f"[ERROR] 喚醒異常: {e}")
        return False


# =============================================================================
# 狀態管理
# =============================================================================


def load_watcher_state() -> Dict:
    """載入 watcher 狀態"""
    state = locked_read_json(WATCHER_STATE_FILE)
    if state is None:
        state = {
            "notified_batches": [],
            "pending_notifications": {},
            "consecutive_failures": 0,
            "last_check": None,
        }
    return state


def save_watcher_state(state: Dict):
    """保存 watcher 狀態"""
    state["last_check"] = datetime.now().isoformat()
    locked_write_json(WATCHER_STATE_FILE, state)


# =============================================================================
# 主監控循環
# =============================================================================


class Watcher:
    """Auto-Wake 監控器"""

    def __init__(
        self,
        check_interval: int = CHECK_INTERVAL_SECONDS,
        notification_delay: int = NOTIFICATION_DELAY_SECONDS,
    ):
        self.check_interval = check_interval
        self.notification_delay = notification_delay
        self.state = load_watcher_state()
        self.running = True

        # 註冊信號處理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """優雅關閉"""
        log("[INFO] 收到停止信號，正在關閉...")
        self.running = False

    def check_once(self) -> List[str]:
        """
        執行一次檢查
        Returns: 本次通知的 batch_id 列表
        """
        notified = []

        # 讀取實驗數據（與 runner 相同的 registry IO 行為）
        data = read_experiments_registry()
        if data is None:
            log("[WARN] 無法讀取 experiments.json")
            return notified

        experiments = list(data.get("experiments", [])) + list(
            data.get("completed", [])
        )
        batches = analyze_batch_status(experiments)

        now = time.time()
        pending = self.state.get("pending_notifications", {})
        already_notified = set(self.state.get("notified_batches", []))

        for batch_id, batch in batches.items():
            # 跳過已通知或非終態
            if batch_id in already_notified:
                continue

            if batch_id in ("archived", "unknown", "legacy"):
                continue

            if not batch.is_complete:
                # 如果之前在等待隊列中但現在有新任務，移除
                if batch_id in pending:
                    del pending[batch_id]
                continue

            # 批次已完成
            if batch_id not in pending:
                # 首次發現完成，加入延遲隊列
                pending[batch_id] = {
                    "discovered_at": now,
                    "notify_after": now + self.notification_delay,
                    "batch_info": {
                        "total": batch.total,
                        "done": batch.done,
                        "error": batch.error,
                    },
                }
                log(
                    f"[INFO] Batch '{batch_id}' 已完成，將於 {self.notification_delay}s 後通知"
                )

            elif now >= pending[batch_id]["notify_after"]:
                # 延遲時間已到，執行通知
                prompt = generate_wake_prompt(batch_id, batch)

                if wake_coder_session(prompt):
                    already_notified.add(batch_id)
                    notified.append(batch_id)

                    # 記錄錯誤到 error_patterns.json（批量更新，縮短競態窗口）
                    failed_entries: List[Tuple[str, Dict]] = []
                    for exp in batch.experiments[:MAX_EXPERIMENTS_PER_BATCH]:
                        if exp.get("status") in ("ERROR", "OOM"):
                            failed_entries.append(
                                (
                                    exp.get("name", "unknown"),
                                    exp.get("error_info", {}) or {},
                                )
                            )
                    update_error_patterns_batch(failed_entries)

                del pending[batch_id]

        # 更新狀態
        self.state["pending_notifications"] = pending
        self.state["notified_batches"] = list(already_notified)
        save_watcher_state(self.state)

        return notified

    def run_loop(self):
        """主監控循環"""
        # Best-effort singleton guard (cross-machine) for loop mode.
        singleton_lock = BASE_DIR / "locks" / "watcher.lock"
        singleton_lock.parent.mkdir(parents=True, exist_ok=True)
        owner_token = f"{socket.gethostname()}|{os.getpid()}|{time.time()}"
        acquired = False
        for _ in range(3):
            try:
                fd = os.open(str(singleton_lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                try:
                    os.write(fd, owner_token.encode("utf-8"))
                finally:
                    os.close(fd)
                acquired = True
                break
            except FileExistsError:
                try:
                    # Parse content-based lock info (hostname|pid|timestamp)
                    try:
                        content = singleton_lock.read_text(
                            encoding="utf-8", errors="ignore"
                        ).strip()
                    except Exception:
                        content = ""

                    lock_hostname = None
                    lock_pid = None
                    lock_time = None

                    # Try new format: hostname|pid|timestamp
                    if "|" in content:
                        parts = content.split("|")
                        if len(parts) >= 3:
                            try:
                                lock_hostname = parts[0]
                                lock_pid = int(parts[1])
                                lock_time = float(parts[2])
                            except (ValueError, IndexError):
                                pass

                    # Fallback: parse old format (pid=... host=... time=...)
                    if lock_pid is None:
                        for part in content.split():
                            if part.startswith("pid="):
                                try:
                                    lock_pid = int(part.split("=", 1)[1])
                                except Exception:
                                    lock_pid = None
                            elif part.startswith("host="):
                                lock_hostname = part.split("=", 1)[1]

                    # Same host: use PID liveness check
                    if lock_hostname == socket.gethostname():
                        if lock_pid:
                            try:
                                os.kill(lock_pid, 0)
                                # Process alive — cannot reclaim
                            except ProcessLookupError:
                                # Dead PID — reclaim
                                log(
                                    f"[WARN] watcher.lock PID 已不存在(pid={lock_pid}) → reclaim: {singleton_lock}"
                                )
                                singleton_lock.unlink()
                                continue
                            except PermissionError:
                                pass
                    else:
                        # Cross-host: use content timestamp
                        if lock_time is not None:
                            age = time.time() - lock_time
                            if age > WATCHER_SINGLETON_STALE_SEC:
                                log(
                                    f"[WARN] watcher.lock stale ({age:.0f}s) → remove: {singleton_lock}"
                                )
                                singleton_lock.unlink()
                                continue
                        else:
                            # Legacy format with no timestamp: use mtime + future guard
                            mtime = singleton_lock.stat().st_mtime
                            now = time.time()
                            if mtime > now + 60:
                                # NFS clock skew: treat as stale
                                log(
                                    f"[WARN] watcher.lock mtime in future (clock skew) → remove: {singleton_lock}"
                                )
                                singleton_lock.unlink()
                                continue
                            age = now - mtime
                            if age > WATCHER_SINGLETON_STALE_SEC:
                                log(
                                    f"[WARN] watcher.lock stale ({age:.0f}s) → remove: {singleton_lock}"
                                )
                                singleton_lock.unlink()
                                continue
                except FileNotFoundError:
                    continue
                except Exception as e:
                    log(f"[WARN] watcher.lock 檢查失敗: {e}")
                log(
                    f"[WARN] watcher.lock 已存在，可能已有 watcher 在跑: {singleton_lock}"
                )
                return
            except Exception as e:
                log(f"[WARN] 無法取得 watcher singleton lock，仍繼續執行: {e}")
                acquired = True
                break

        if not acquired:
            log(f"[WARN] 無法取得 watcher singleton lock: {singleton_lock}")
            return

        log("=" * 60)
        log("Auto-Wake Watcher 啟動")
        log(f"  檢查間隔: {self.check_interval}s")
        log(f"  通知延遲: {self.notification_delay}s")
        log(f"  安全限制: 每批次 {MAX_EXPERIMENTS_PER_BATCH} 個實驗")
        log(f"  連續失敗上限: {MAX_CONSECUTIVE_FAILURES} 次")
        log("=" * 60)

        while self.running:
            try:
                notified = self.check_once()

                # 成功一輪就重置連續失敗計數
                self.state["consecutive_failures"] = 0

                if notified:
                    log(f"[OK] 已通知批次: {', '.join(notified)}")

            except Exception as e:
                log(f"[ERROR] 監控異常: {e}")
                self.state["consecutive_failures"] = (
                    self.state.get("consecutive_failures", 0) + 1
                )

                if self.state["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES:
                    log(f"[FATAL] 連續失敗 {MAX_CONSECUTIVE_FAILURES} 次，停止監控")
                    break

            # 等待下一次檢查
            for _ in range(self.check_interval):
                if not self.running:
                    break
                time.sleep(1)

        save_watcher_state(self.state)
        log("[INFO] Watcher 已停止")

        try:
            if singleton_lock.exists():
                try:
                    content = singleton_lock.read_text(
                        encoding="utf-8", errors="ignore"
                    )
                except Exception:
                    content = ""
                if owner_token in content:
                    singleton_lock.unlink()
        except Exception:
            pass


# =============================================================================
# CLI 入口
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Auto-Wake Watcher - 實驗批次完成監控系統",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
    python watcher.py                    # 啟動持續監控
    python watcher.py --once             # 單次檢查
    python watcher.py --interval 60      # 每 60 秒檢查一次
    python watcher.py --delay 300        # 完成後等待 5 分鐘再通知
    python watcher.py --status           # 顯示當前狀態
    python watcher.py --reset            # 重置已通知列表
        """,
    )

    parser.add_argument("--once", action="store_true", help="單次檢查後退出")
    parser.add_argument(
        "--interval",
        type=int,
        default=CHECK_INTERVAL_SECONDS,
        help=f"檢查間隔秒數 (預設: {CHECK_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=NOTIFICATION_DELAY_SECONDS,
        help=f"通知延遲秒數 (預設: {NOTIFICATION_DELAY_SECONDS})",
    )
    parser.add_argument("--status", action="store_true", help="顯示當前監控狀態")
    parser.add_argument("--reset", action="store_true", help="重置已通知批次列表")
    add_common_args(parser)

    args = parser.parse_args()
    setup_logging(args)

    if args.dry_run:
        emit_result(
            args,
            {
                "dry_run": True,
                "interval": args.interval,
                "delay": args.delay,
                "once": args.once,
            },
        )
        return
    if args.status:
        state = load_watcher_state()
        print("\n=== Watcher 狀態 ===")
        print(f"已通知批次: {state.get('notified_batches', [])}")
        print(f"待通知隊列: {list(state.get('pending_notifications', {}).keys())}")
        print(f"連續失敗數: {state.get('consecutive_failures', 0)}")
        print(f"上次檢查: {state.get('last_check', 'N/A')}")

        # 顯示當前批次狀態
        data = read_experiments_registry()
        if data:
            batches = analyze_batch_status(data.get("experiments", []))
            print("\n=== 批次狀態 ===")
            for bid, b in sorted(batches.items()):
                status = "✅ 完成" if b.is_complete else "🔄 進行中"
                print(
                    f"  {bid}: {status} ({b.done}/{b.total} 成功, {b.error} 失敗, {b.running} 運行中)"
                )
        return

    # 重置
    if args.reset:
        state = load_watcher_state()
        state["notified_batches"] = []
        state["pending_notifications"] = {}
        state["consecutive_failures"] = 0
        save_watcher_state(state)
        print("[OK] 已重置 watcher 狀態")
        return

    # 單次檢查
    if args.once:
        watcher = Watcher(
            check_interval=args.interval,
            notification_delay=0,  # 單次模式不延遲
        )
        notified = watcher.check_once()
        if notified:
            print(f"[OK] 已通知批次: {', '.join(notified)}")
        else:
            print("[INFO] 無需通知")
        return

    # 持續監控
    watcher = Watcher(check_interval=args.interval, notification_delay=args.delay)
    watcher.run_loop()


if __name__ == "__main__":
    main()
