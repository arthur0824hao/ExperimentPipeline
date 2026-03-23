import json
import logging
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

try:
    from artifact import _load_json_dict
    from formatting import _parse_iso_ts, format_time_ago
    from runtime_config import cfg_int, get_runtime_section

    if TYPE_CHECKING:
        from db_registry import DBExperimentsDB
except ModuleNotFoundError:
    from pipeline.artifact import _load_json_dict
    from pipeline.formatting import _parse_iso_ts, format_time_ago
    from pipeline.runtime_config import cfg_int, get_runtime_section

    if TYPE_CHECKING:
        from pipeline.db_registry import DBExperimentsDB

BASE_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = BASE_DIR.parent
_DEFAULT_MACHINES_FILE = PROJECT_ROOT / "configs" / "machines.json"
MACHINES_FILES = [
    PROJECT_ROOT / "configs" / "machines.json",
    PROJECT_ROOT / "configs" / "machines.phase3.json",
]
MACHINES_FILE = _DEFAULT_MACHINES_FILE
HEARTBEATS_DIR = BASE_DIR / "heartbeats"
_RUNNER_CFG = get_runtime_section("experiments_runner")
HEARTBEAT_STALE_SEC = cfg_int(_RUNNER_CFG, "heartbeat_stale_sec", 120)


class ClusterManager:
    def __init__(self):
        self.machines = self.load_machines()

    def load_machines(self):
        machine_paths = (
            [Path(MACHINES_FILE)]
            if MACHINES_FILE != _DEFAULT_MACHINES_FILE
            else MACHINES_FILES
        )
        for config_path in machine_paths:
            if not config_path.exists():
                continue
            try:
                with open(config_path, "r") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    return data
            except Exception:
                continue
        return {}

    def _load_heartbeat_files(self) -> Dict[str, Dict[str, Any]]:
        heartbeats: Dict[str, Dict[str, Any]] = {}
        now = time.time()
        if not HEARTBEATS_DIR.exists():
            return heartbeats

        for hb_path in HEARTBEATS_DIR.glob("*.json"):
            payload = _load_json_dict(hb_path)
            if not payload:
                continue

            worker_id = payload.get("worker_id")
            if not isinstance(worker_id, str) or not worker_id:
                worker_id = hb_path.stem

            if not worker_id:
                continue

            hb: Dict[str, Any] = dict(payload)
            if "last_seen_sec" not in hb:
                last_seen_ts = _parse_iso_ts(payload.get("timestamp"))
                hb["last_seen_sec"] = (
                    now - last_seen_ts if last_seen_ts is not None else 999999
                )
            heartbeats[worker_id] = hb

        return heartbeats

    def get_cluster_status(self, db: Optional["DBExperimentsDB"] = None) -> Dict:
        status_map = {}

        for mid, conf in self.machines.items():
            status_map[mid] = {
                "status": "OFFLINE",
                "last_seen_sec": 999999,
                "config": conf,
                "is_known": True,
                "gpus": [],
                "cpu": {},
                "running_jobs": 0,
                "running_experiments": [],
            }

        heartbeats = db.get_cluster_heartbeats() if db else self._load_heartbeat_files()
        for w_id, hb in heartbeats.items():
            seconds_ago = hb.get("last_seen_sec", 999999)
            is_online = seconds_ago < 60

            if w_id not in status_map:
                status_map[w_id] = {"config": {}, "is_known": False}

            status_map[w_id].update(
                {
                    "status": "ONLINE" if is_online else "OFFLINE",
                    "last_seen_sec": seconds_ago,
                    "gpus": hb.get("gpus", []),
                    "cpu": hb.get("cpu", {}),
                    "gpu_probe_error": str(hb.get("gpu_probe_error", "") or ""),
                    "running_jobs": hb.get("running_jobs", 0),
                    "running_experiments": hb.get("running_experiments", []),
                    "pid": hb.get("pid"),
                }
            )

        return status_map

    def _ssh_base_cmd(self, host: str) -> List[str]:
        return [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "StrictHostKeyChecking=accept-new",
            host,
        ]

    def start_node(self, node_id):
        if node_id not in self.machines:
            return False, f"Unknown node: {node_id}"

        conf = self.machines[node_id]
        host = conf["host"]
        session = conf.get("tmux_session", "exp_runner")
        work_dir = conf.get("work_dir", str(BASE_DIR))

        runner_cmd = (
            f"source ~/miniconda3/etc/profile.d/conda.sh && "
            f"conda activate gnn_fraud && "
            f"cd {work_dir} && "
            f"python experiments.py --worker_id {node_id}; "
            f"echo 'Runner exited. Press Enter...'; read"
        )

        full_cmd = (
            f"tmux kill-session -t {session} 2>/dev/null; "
            f"tmux new-session -d -s {session} bash -c '{runner_cmd}'"
        )

        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host), full_cmd],
                timeout=15,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return True, f"Started {node_id}"
            else:
                return False, f"SSH error: {result.stderr[:100]}"
        except subprocess.TimeoutExpired:
            return False, "SSH timeout"
        except Exception as e:
            return False, str(e)

    def stop_node(self, node_id):
        if node_id not in self.machines:
            return False, f"Unknown node: {node_id}"

        conf = self.machines[node_id]
        host = conf["host"]
        session = conf.get("tmux_session", "exp_runner")

        try:
            # Step 1: Kill all Phase3 training processes on the remote node
            # Covers: train.py, train_parallel_optimized.py, and any python child processes
            kill_cmd = (
                f"pkill -TERM -f 'Phase3/experiments/.*/scripts/train' 2>/dev/null; "
                f"pkill -TERM -f 'train_parallel_optimized\\.py' 2>/dev/null; "
                f"sleep 1; "
                f"pkill -9 -f 'Phase3/experiments/.*/scripts/train' 2>/dev/null; "
                f"pkill -9 -f 'train_parallel_optimized\\.py' 2>/dev/null; "
                f"pkill -TERM -f 'experiments\\.py --worker_id {node_id}' 2>/dev/null; "
                f"sleep 1; "
                f"pkill -9 -f 'experiments\\.py --worker_id {node_id}' 2>/dev/null; "
                f"tmux kill-session -t {session} 2>/dev/null"
            )
            result = subprocess.run(
                [*self._ssh_base_cmd(host), kill_cmd],
                timeout=15,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return True, f"Stopped {node_id} (processes + tmux)"
            stderr = (result.stderr or "").strip()
            if not stderr:
                stderr = (result.stdout or "").strip()
            return False, f"SSH stop failed: {stderr[:120]}"
        except subprocess.TimeoutExpired:
            return False, "SSH timeout"
        except Exception as e:
            return False, str(e)

    def restart_node(self, node_id):
        return self.start_node(node_id)

    def kill_remote_pid(self, node_id: str, pid: int) -> Tuple[bool, str]:
        if node_id not in self.machines:
            return False, f"Unknown node: {node_id}"
        conf = self.machines[node_id]
        host = conf["host"]
        cmd = (
            f"kill -TERM {int(pid)} 2>/dev/null; "
            f"sleep 1; "
            f"kill -9 {int(pid)} 2>/dev/null"
        )
        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host), cmd],
                timeout=10,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return True, f"Killed remote PID {pid}"
            return False, f"SSH kill failed: {result.stderr[:120]}"
        except subprocess.TimeoutExpired:
            return False, "SSH timeout"
        except Exception as e:
            return False, str(e)
