import json
import logging
import platform
import re
import shlex
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

try:
    from artifact import _load_json_dict
    from formatting import _parse_iso_ts, format_time_ago
    from gpu import _coerce_nvidia_int
    from runtime_config import cfg_int, get_runtime_section

    if TYPE_CHECKING:
        from db_registry import DBExperimentsDB
except ModuleNotFoundError:
    from pipeline.artifact import _load_json_dict
    from pipeline.formatting import _parse_iso_ts, format_time_ago
    from pipeline.gpu import _coerce_nvidia_int
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
HEARTBEAT_ONLINE_SEC = cfg_int(_RUNNER_CFG, "heartbeat_online_sec", 60)
START_HEARTBEAT_WAIT_SEC = cfg_int(_RUNNER_CFG, "start_heartbeat_wait_sec", 20)
START_HEARTBEAT_POLL_SEC = cfg_int(_RUNNER_CFG, "start_heartbeat_poll_sec", 2)
GRACEFUL_STOP_WAIT_SEC = cfg_int(_RUNNER_CFG, "graceful_stop_wait_sec", 12)


class ClusterManager:
    def __init__(self):
        self.machines = self.load_machines()
        self._probe_cache: Dict[str, Dict[str, Any]] = {}
        self._probe_cache_lock = threading.Lock()
        self._probe_cache_ttl_sec = 60.0

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
                "our_gpu_ids": [],
            }

        heartbeats = db.get_cluster_heartbeats() if db else self._load_heartbeat_files()
        for w_id, hb in heartbeats.items():
            seconds_ago = hb.get("last_seen_sec", 999999)
            is_online = seconds_ago < HEARTBEAT_ONLINE_SEC

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

        now = time.time()

        # Inject cached probe data for disabled machines
        if db:
            for w_id, info in status_map.items():
                if not db.is_worker_disabled(w_id):
                    continue
                probe = self._get_probe_cache(w_id, now=now)
                if not probe:
                    continue
                gpus = probe.get("gpus")
                cpu = probe.get("cpu")
                if isinstance(gpus, list):
                    info["gpus"] = gpus
                if isinstance(cpu, dict):
                    info["cpu"] = cpu
                if "gpu_probe_error" in probe:
                    info["gpu_probe_error"] = str(probe.get("gpu_probe_error") or "")

        # Query DB for GPU ownership (which GPUs run our experiments)
        worker_gpu_ids: Dict[str, Set[int]] = {}
        if db:
            try:
                from db_registry import get_conn

                with get_conn(getattr(db, "dsn", None)) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT worker_id, gpu_id
                            FROM exp_registry.experiments
                            WHERE status = 'RUNNING'
                              AND worker_id IS NOT NULL
                              AND gpu_id IS NOT NULL
                            """
                        )
                        rows = cur.fetchall() or []
                for worker_id, gpu_id in rows:
                    wid = str(worker_id or "").strip()
                    if not wid:
                        continue
                    try:
                        gid = int(gpu_id)
                    except (TypeError, ValueError):
                        continue
                    worker_gpu_ids.setdefault(wid, set()).add(gid)
            except Exception:
                worker_gpu_ids = {}

        for w_id, info in status_map.items():
            gpu_ids = sorted(worker_gpu_ids.get(w_id, set()))
            info["our_gpu_ids"] = gpu_ids

        return status_map

    def _set_probe_cache(
        self, node_id: str, probe: Dict[str, Any], now: Optional[float] = None
    ) -> None:
        ts = now if now is not None else time.time()
        payload = dict(probe)
        payload["probed_at"] = ts
        with self._probe_cache_lock:
            self._probe_cache[node_id] = payload

    def _get_probe_cache(
        self, node_id: str, now: Optional[float] = None
    ) -> Optional[Dict[str, Any]]:
        ts = now if now is not None else time.time()
        with self._probe_cache_lock:
            cached = self._probe_cache.get(node_id)
            if not isinstance(cached, dict):
                return None
            probed_at = float(cached.get("probed_at") or 0.0)
            if ts - probed_at > self._probe_cache_ttl_sec:
                self._probe_cache.pop(node_id, None)
                return None
            return dict(cached)

    def _probe_machine_stats(self, node_id: str) -> Optional[Dict[str, Any]]:
        """SSH into a (possibly disabled) machine and collect GPU/CPU stats."""
        conf = self.machines.get(node_id, {})
        if not isinstance(conf, dict):
            return None
        host = conf.get("host")
        if not host:
            return None
        port = conf.get("ssh_port")

        probe_cmd = (
            "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu "
            "--format=csv,noheader,nounits 2>/dev/null; "
            "echo '---'; "
            "uptime"
        )
        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host, port), probe_cmd],
                timeout=8,
                capture_output=True,
                text=True,
            )
        except Exception:
            return None

        output = (result.stdout or "").strip()
        if result.returncode != 0 and not output:
            return None
        if not output:
            return None

        parts = re.split(r"(?m)^---\s*$", output, maxsplit=1)
        gpu_part = parts[0].strip() if parts else ""
        uptime_part = parts[1].strip() if len(parts) > 1 else ""

        gpus: List[Dict[str, int]] = []
        for line in gpu_part.splitlines():
            if not line.strip():
                continue
            cols = [part.strip() for part in line.split(",")]
            if len(cols) < 4:
                continue
            try:
                idx = _coerce_nvidia_int(cols[0])
                used = _coerce_nvidia_int(cols[1])
                total = _coerce_nvidia_int(cols[2])
                util = _coerce_nvidia_int(cols[3])
            except Exception:
                continue
            gpus.append(
                {
                    "index": idx,
                    "free": max(0, total - used),
                    "used": used,
                    "total": total,
                    "util": util,
                }
            )

        load1 = 0.0
        load5 = 0.0
        load15 = 0.0
        if uptime_part:
            match = re.search(
                r"load averages?:\s*([0-9]+(?:\.[0-9]+)?)\s*[, ]\s*"
                r"([0-9]+(?:\.[0-9]+)?)\s*[, ]\s*([0-9]+(?:\.[0-9]+)?)",
                uptime_part,
            )
            if match:
                try:
                    load1 = float(match.group(1))
                    load5 = float(match.group(2))
                    load15 = float(match.group(3))
                except Exception:
                    pass

        cpu_count = max(1, int(conf.get("cpu_count") or 1))
        cpu = {
            "load1": round(load1, 2),
            "load5": round(load5, 2),
            "load15": round(load15, 2),
            "cpu_count": cpu_count,
            "load_percent": round(
                min(100.0, max(0.0, load1 / cpu_count * 100.0)), 1
            ),
        }
        return {"gpus": gpus, "cpu": cpu, "gpu_probe_error": ""}

    def _ssh_base_cmd(self, host: str, port: Optional[int] = None) -> List[str]:
        cmd = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "StrictHostKeyChecking=accept-new",
        ]
        if port:
            cmd.extend(["-p", str(port)])
        cmd.append(host)
        return cmd

    def _setup_db_tunnel(self, conf: Dict[str, Any]) -> None:
        """Set up reverse SSH tunnel for DB access on remote machines."""
        host = conf.get("host")
        ssh_port = conf.get("ssh_port")
        tunnel_port = conf.get("db_tunnel_port")
        if not (host and ssh_port and tunnel_port):
            return
        tunnel_cmd = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "ServerAliveInterval=30",
            "-p",
            str(ssh_port),
            "-fNR",
            f"{tunnel_port}:localhost:5432",
            f"{conf.get('user', 'arthur0824hao')}@{host}",
        ]
        try:
            subprocess.run(tunnel_cmd, timeout=10, capture_output=True)
        except Exception:
            pass

    def _is_remote_runner_alive(
        self, node_id: str, host: str, port: Optional[int], session: str
    ) -> bool:
        probe_cmd = (
            f"tmux has-session -t {session} 2>/dev/null && "
            f"pgrep -f 'python experiments.py --worker_id {node_id}' >/dev/null"
        )
        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host, port), probe_cmd],
                timeout=8,
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _is_remote_worker_pid_alive(
        self, node_id: str, host: str, port: Optional[int]
    ) -> bool:
        probe_cmd = f"pgrep -f 'python experiments.py --worker_id {node_id}' >/dev/null"
        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host, port), probe_cmd],
                timeout=8,
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _is_local_target(self, host: str) -> bool:
        token = str(host or "").strip().lower()
        if not token:
            return False
        local_hosts = {
            "localhost",
            "127.0.0.1",
            "::1",
            str(platform.node() or "").strip().lower(),
            str(socket.gethostname() or "").strip().lower(),
            str(socket.getfqdn() or "").strip().lower(),
        }
        normalized_local_hosts = set()
        for item in local_hosts:
            if not item:
                continue
            normalized_local_hosts.add(item)
            normalized_local_hosts.add(item.split(".")[0])
        return token in normalized_local_hosts or token.split(".")[0] in normalized_local_hosts

    def _is_heartbeat_fresh(
        self, node_id: str, db: Optional["DBExperimentsDB"] = None
    ) -> Tuple[bool, Optional[int]]:
        try:
            if db:
                heartbeats = db.get_cluster_heartbeats()
            else:
                heartbeats = self._load_heartbeat_files()
            
            hb = heartbeats.get(node_id, {})
            last_seen_sec = hb.get("last_seen_sec")
            
            if last_seen_sec is None:
                return (False, None)

            is_fresh = last_seen_sec < HEARTBEAT_STALE_SEC
            return (is_fresh, last_seen_sec)
            
        except Exception:
            return (False, None)

    def _wait_for_heartbeat_resume(
        self,
        node_id: str,
        db: Optional["DBExperimentsDB"],
        timeout_sec: int = START_HEARTBEAT_WAIT_SEC,
    ) -> Tuple[bool, Optional[int]]:
        if db is None:
            return (True, None)
        deadline = time.time() + max(1, timeout_sec)
        poll = max(1, START_HEARTBEAT_POLL_SEC)
        while time.time() <= deadline:
            try:
                hb = db.get_cluster_heartbeats().get(node_id, {})
                last_seen = hb.get("last_seen_sec")
                if last_seen is not None and last_seen < HEARTBEAT_ONLINE_SEC:
                    return (True, int(last_seen))
            except Exception:
                pass
            time.sleep(poll)
        return (False, None)

    def _build_runner_launcher(self, node_id: str, work_dir: str, db_env: str) -> Tuple[str, str]:
        launcher_path = f"{work_dir}/.runner_launch_{node_id}.sh"
        runner_log = f"{work_dir}/logs/worker_{node_id}.log"
        launcher_lines = [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "source ~/miniconda3/etc/profile.d/conda.sh",
            "conda activate gnn_fraud",
        ]
        if db_env:
            launcher_lines.append(db_env)
        launcher_lines.extend(
            [
                f"cd {shlex.quote(work_dir)}",
                f"python experiments.py --worker_id {shlex.quote(node_id)} >> {shlex.quote(runner_log)} 2>&1",
            ]
        )
        launcher_content = "\n".join(launcher_lines) + "\n"
        launcher_setup_cmd = (
            f"cat > {shlex.quote(launcher_path)} <<'EOF'\n"
            f"{launcher_content}"
            "EOF\n"
            f"chmod +x {shlex.quote(launcher_path)}"
        )
        return launcher_path, launcher_setup_cmd

    def start_node(self, node_id, force_restart: bool = False, db: Optional["DBExperimentsDB"] = None):
        if node_id not in self.machines:
            return False, f"Unknown node: {node_id}"

        conf = self.machines[node_id]
        host = conf.get("host")
        if not host:
            return False, f"No host configured for {node_id}"
        port = conf.get("ssh_port")
        session = conf.get("tmux_session", "exp_runner")
        work_dir = conf.get("work_dir", str(BASE_DIR))
        restart_stale = False

        try:
            max_gpus = int(conf.get("max_gpus"))
        except (TypeError, ValueError):
            max_gpus = None
        if max_gpus is not None and max_gpus <= 0 and self._is_local_target(str(host)):
            return (
                False,
                f"Refusing to start {node_id}: this machine (max_gpus=0) is management-only",
            )

        if not force_restart:
            runner_alive = self._is_remote_runner_alive(node_id, host, port, session)
            if runner_alive:
                heartbeat_fresh, last_seen = self._is_heartbeat_fresh(node_id, db)
                if heartbeat_fresh:
                    return True, f"Already running {node_id} (fresh heartbeat)"
                restart_stale = True
            else:
                pid_alive = self._is_remote_worker_pid_alive(node_id, host, port)
                if pid_alive:
                    heartbeat_fresh, last_seen = self._is_heartbeat_fresh(node_id, db)
                    if heartbeat_fresh:
                        return (
                            True,
                            f"Already running {node_id} (fresh heartbeat, unmanaged tmux)",
                        )
                    restart_stale = True

        self._setup_db_tunnel(conf)

        db_env = ""
        tunnel_port = conf.get("db_tunnel_port")
        if tunnel_port:
            db_env = f"export EXP_PGHOST=localhost EXP_PGPORT={int(tunnel_port)}"
        launcher_path, launcher_setup_cmd = self._build_runner_launcher(
            node_id=node_id,
            work_dir=str(work_dir),
            db_env=db_env,
        )

        graceful_restart_cmd = (
            f"set +e; "
            f"old_pid=$(pgrep -f '[e]xperiments\\.py --worker_id {node_id}' | tr '\\n' ' '); "
            "if [ -n \"$old_pid\" ]; then "
            "echo \"Graceful shutdown requested for worker PIDs: $old_pid\"; "
            "kill -TERM $old_pid 2>/dev/null || true; "
            f"for _ in $(seq 1 {max(1, GRACEFUL_STOP_WAIT_SEC)}); do "
            f"if ! pgrep -f '[e]xperiments\\.py --worker_id {node_id}' >/dev/null; then break; fi; "
            "sleep 1; "
            "done; "
            "fi; "
            f"pkill -KILL -f '[e]xperiments\\.py --worker_id {node_id}' 2>/dev/null || true; "
            f"tmux kill-session -t {session} 2>/dev/null || true; "
            "true"
        )

        if force_restart or restart_stale:
            full_cmd = (
                f"{graceful_restart_cmd}; "
                f"{launcher_setup_cmd}; "
                f"tmux new-session -d -s {shlex.quote(str(session))} {shlex.quote(launcher_path)}"
            )
        else:
            full_cmd = (
                f"if pgrep -f 'python experiments.py --worker_id {node_id}' >/dev/null; then "
                f"echo 'Runner already alive'; exit 0; fi; "
                f"{launcher_setup_cmd}; "
                f"tmux kill-session -t {session} 2>/dev/null || true; "
                f"tmux new-session -d -s {shlex.quote(str(session))} {shlex.quote(launcher_path)}"
            )

        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host, port), full_cmd],
                timeout=15,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                resumed, age = self._wait_for_heartbeat_resume(node_id, db)
                if resumed:
                    age_text = f"{age}s" if age is not None else "ok"
                    if force_restart or restart_stale:
                        return (
                            True,
                            f"Started {node_id} after graceful shutdown attempt (heartbeat fresh: {age_text})",
                        )
                    return True, f"Started {node_id} (heartbeat fresh: {age_text})"
                return False, f"Started {node_id} but heartbeat not fresh within {START_HEARTBEAT_WAIT_SEC}s"
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
        host = conf.get("host")
        if not host:
            return False, f"No host configured for {node_id}"
        port = conf.get("ssh_port")
        session = conf.get("tmux_session", "exp_runner")

        try:
            kill_cmd = (
                "set +e; "
                f"pkill -TERM -f 'Phase3/experiments/.*/scripts/[t]rain' 2>/dev/null || true; "
                f"pkill -TERM -f 'Experiment/experiments/.*/scripts/[t]rain' 2>/dev/null || true; "
                f"pkill -TERM -f '[t]rain_parallel_optimized\\.py' 2>/dev/null || true; "
                f"sleep 1; "
                f"pkill -9 -f 'Phase3/experiments/.*/scripts/[t]rain' 2>/dev/null || true; "
                f"pkill -9 -f 'Experiment/experiments/.*/scripts/[t]rain' 2>/dev/null || true; "
                f"pkill -9 -f '[t]rain_parallel_optimized\\.py' 2>/dev/null || true; "
                f"pkill -TERM -f '[e]xperiments\\.py --worker_id {node_id}' 2>/dev/null || true; "
                f"sleep 1; "
                f"pkill -9 -f '[e]xperiments\\.py --worker_id {node_id}' 2>/dev/null || true; "
                f"tmux kill-session -t {session} 2>/dev/null || true; "
                "exit 0"
            )
            result = subprocess.run(
                [*self._ssh_base_cmd(host, port), kill_cmd],
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

    def restart_node(self, node_id, db: Optional["DBExperimentsDB"] = None):
        return self.start_node(node_id, force_restart=True, db=db)

    def kill_remote_pid(self, node_id: str, pid: int) -> Tuple[bool, str]:
        if node_id not in self.machines:
            return False, f"Unknown node: {node_id}"
        conf = self.machines[node_id]
        host = conf.get("host")
        if not host:
            return False, f"No host configured for {node_id}"
        port = conf.get("ssh_port")
        cmd = (
            f"kill -TERM {int(pid)} 2>/dev/null; "
            f"sleep 1; "
            f"kill -9 {int(pid)} 2>/dev/null"
        )
        try:
            result = subprocess.run(
                [*self._ssh_base_cmd(host, port), cmd],
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
