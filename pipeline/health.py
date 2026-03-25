#!/usr/bin/env python3

import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

try:
    from cluster import ClusterManager
    from db_registry import DBExperimentsDB
    from formatting import normalize_status
    from logger_hybrid import HybridLogger
    from runtime_config import cfg_bool, cfg_int, get_runtime_section
except ModuleNotFoundError:
    from .cluster import ClusterManager
    from .db_registry import DBExperimentsDB
    from .formatting import normalize_status
    from .logger_hybrid import HybridLogger
    from .runtime_config import cfg_bool, cfg_int, get_runtime_section


BASE_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = BASE_DIR.parent
RESULTS_DB_DIR = BASE_DIR / "results_db"
LOGS_DIR = BASE_DIR / "logs"

_RUNNER_CFG = get_runtime_section("experiments_runner")
_SENTINEL_CFG = get_runtime_section("sentinel")

HEARTBEAT_STALE_SEC = cfg_int(_RUNNER_CFG, "heartbeat_stale_sec", 120)
SENTINEL_ENABLED = cfg_bool(_SENTINEL_CFG, "enabled", True)
SENTINEL_BUDDY_REPORT_TTL_SEC = cfg_int(_SENTINEL_CFG, "buddy_report_ttl_sec", 90)
ORPHAN_REAPER_INTERVAL_SEC = cfg_int(_RUNNER_CFG, "orphan_reaper_interval_sec", 30)
ORPHAN_ETIMES_SEC = cfg_int(_RUNNER_CFG, "orphan_etimes_sec", 120)
ORPHAN_CONFIRMATION_SEC = cfg_int(_RUNNER_CFG, "orphan_confirmation_sec", 30)
ORPHAN_TRAINING_SEEN: Dict[int, float] = {}

STATUS_NEEDS_RERUN = "NEEDS_RERUN"
STATUS_RUNNING = "RUNNING"

ExperimentsDB = DBExperimentsDB


def _clean_experiment_artifacts(exp_name: str) -> List[str]:
    exp_dir = BASE_DIR / "experiments" / exp_name
    targets = [
        exp_dir / ".progress",
        exp_dir / "resource_usage.json",
        exp_dir / "outputs",
        exp_dir / "results_db",
        exp_dir / "checkpoints",
        RESULTS_DB_DIR / f"{exp_name}.json",
        LOGS_DIR / f"{exp_name}.out",
        LOGS_DIR / f"{exp_name}.err",
    ]
    pycache_dirs = list(exp_dir.rglob("__pycache__"))
    pyc_files = list(exp_dir.rglob("*.pyc"))
    targets.extend(pycache_dirs)
    targets.extend(pyc_files)
    for scripts_pycache in (BASE_DIR / "scripts").rglob("__pycache__"):
        targets.append(scripts_pycache)
    removed: List[str] = []
    for path in targets:
        try:
            if path.is_dir():
                for attempt in range(2):
                    try:
                        shutil.rmtree(path)
                        break
                    except OSError as e:
                        if e.errno != 39 or attempt == 1:
                            raise
                        time.sleep(0.1)
                removed.append(str(path.relative_to(BASE_DIR)))
            elif path.exists():
                path.unlink()
                removed.append(str(path.relative_to(BASE_DIR)))
        except FileNotFoundError:
            continue
    return removed


def cleanup_on_startup(logger):
    try:
        from registry_io import cleanup_orphan_files
    except ModuleNotFoundError:
        from .registry_io import cleanup_orphan_files

    db_path = BASE_DIR / "experiments.json"
    removed = cleanup_orphan_files(db_path, max_age_sec=3600)
    if removed:
        logger.log(f"Startup cleanup: removed {removed} orphan .tmp/.nfs files")


def _get_active_runner_pids_from_db(
    db: Optional[ExperimentsDB] = None,
    stale_sec: int = HEARTBEAT_STALE_SEC,
) -> Set[int]:
    active_pids: Set[int] = set()
    if db is None:
        return active_pids
    try:
        heartbeats = db.get_filtered_cluster_heartbeats()
        for wid, info in heartbeats.items():
            if info.get("last_seen_sec", 999999) > stale_sec:
                continue
            pid = info.get("pid")
            if isinstance(pid, int) and pid > 1:
                active_pids.add(pid)
    except Exception:
        pass
    return active_pids


def _kill_local_pid_tree(pid: int) -> bool:
    try:
        subprocess.run(
            ["pkill", "-TERM", "-P", str(pid)],
            timeout=2,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        pass
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except Exception:
        pass
    time.sleep(0.8)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except Exception:
        return False
    try:
        subprocess.run(
            ["pkill", "-KILL", "-P", str(pid)],
            timeout=2,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        pass
    try:
        os.kill(pid, signal.SIGKILL)
        return True
    except ProcessLookupError:
        return True
    except Exception:
        return False


def enforce_running_pid_registration(db: ExperimentsDB, logger, grace_sec: int = 20):
    fixed = db.enforce_running_pid_registration(grace_sec)
    for name in fixed:
        logger.log(f"PID registration missing, reset to NEEDS_RERUN: {name}")


def reap_orphan_runner_processes(
    logger,
    current_runner_pid: int,
    db: Optional[ExperimentsDB] = None,
    worker_id: Optional[str] = None,
):
    active_runner_pids = _get_active_runner_pids_from_db(db)
    try:
        proc = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,etimes=,args="],
            timeout=5,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        logger.log(f"Orphan reaper skipped: {e}")
        return
    killed = 0
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
            etimes = int(parts[2])
        except ValueError:
            continue
        args = parts[3]
        if pid <= 1 or pid == current_runner_pid:
            continue
        if ppid != 1 or etimes < ORPHAN_ETIMES_SEC:
            continue
        if "experiments.py" not in args:
            continue
        if "python experiments.py" not in args and "python3 experiments.py" not in args:
            continue
        if "--worker_id" not in args and "--watch" not in args:
            continue
        if worker_id and f"--worker_id {worker_id}" not in args:
            continue
        if pid in active_runner_pids:
            continue
        ok = _kill_local_pid_tree(pid)
        if ok:
            killed += 1
            logger.log(f"Reaped orphan runner/watcher PID {pid}: {args[:140]}")
    if killed:
        logger.log(f"Orphan runner reaper killed {killed} process(es)")


def _extract_exp_name_from_cmd(args: str) -> Optional[str]:
    try:
        tokens = shlex.split(args)
    except Exception:
        tokens = []
    if tokens:
        for i, tok in enumerate(tokens):
            if tok == "--experiment-name" and i + 1 < len(tokens):
                name = tokens[i + 1].strip()
                if name:
                    return name
    marker2 = "/Phase3/experiments/"
    marker3 = "/scripts/train.py"
    if marker2 in args and marker3 in args:
        tail = args.split(marker2, 1)[1]
        prefix = tail.split(marker3, 1)[0]
        if prefix:
            return prefix.split("/", 1)[0]
    return None


def reap_orphan_training_processes(
    db: ExperimentsDB,
    logger,
    worker_id: str,
    active_experiment_names: Set[str],
):
    try:
        proc = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,etimes=,args="],
            timeout=5,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        logger.log(f"Orphan training reaper skipped: {e}")
        return

    local_hb_running: Set[str] = set()
    try:
        hb_data = db.get_worker_heartbeat(worker_id)
        hb_running = hb_data.get("running_experiments") or []
        if isinstance(hb_running, list):
            local_hb_running = {str(x) for x in hb_running if str(x)}
    except Exception:
        pass

    try:
        snapshot = db.load()
    except Exception as e:
        logger.log(f"Orphan training reaper skipped (registry unreadable): {e}")
        return

    experiments_by_name: Dict[str, Dict[str, Any]] = {
        str(exp.get("name", "")): exp
        for exp in snapshot.get("experiments", [])
        if isinstance(exp, dict) and exp.get("name")
    }

    allowed_names = set(active_experiment_names) | local_hb_running
    killed = 0
    now = time.time()
    live_pids: Set[int] = set()
    for raw in proc.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
            etimes = int(parts[2])
        except ValueError:
            continue
        live_pids.add(pid)
        args = parts[3]
        if ppid != 1 or etimes < ORPHAN_ETIMES_SEC:
            continue
        is_training_cmd = "train_ensemble_member.py" in args or (
            "/Phase3/experiments/" in args and "/scripts/train.py" in args
        )
        if not is_training_cmd:
            continue
        exp_name = _extract_exp_name_from_cmd(args)
        if not exp_name:
            continue
        if exp_name not in experiments_by_name:
            logger.log(
                f"Orphan training candidate skipped (unknown experiment): pid={pid} exp={exp_name}"
            )
            continue

        exp_meta = experiments_by_name[exp_name]
        running_on = exp_meta.get("running_on") or {}
        reg_worker = str(running_on.get("worker", ""))
        if (
            normalize_status(exp_meta.get("status")) == STATUS_RUNNING
            and reg_worker
            and reg_worker != worker_id
        ):
            logger.log(
                f"Orphan training candidate skipped (owned by other worker): pid={pid} exp={exp_name} owner={reg_worker}"
            )
            continue
        if exp_name in allowed_names:
            ORPHAN_TRAINING_SEEN.pop(pid, None)
            continue

        first_seen = ORPHAN_TRAINING_SEEN.get(pid)
        if first_seen is None:
            ORPHAN_TRAINING_SEEN[pid] = now
            logger.log(
                f"Orphan training candidate first-seen pid={pid} exp={exp_name}; waiting confirmation"
            )
            continue
        if now - first_seen < ORPHAN_CONFIRMATION_SEC:
            continue

        ok = _kill_local_pid_tree(pid)
        if not ok:
            logger.log(f"Orphan training reap FAILED pid={pid} exp={exp_name}")
            continue
        ORPHAN_TRAINING_SEEN.pop(pid, None)

        killed += 1
        logger.log(f"Reaped orphan training PID {pid} exp={exp_name}")

        try:
            ok = db.update_experiment(
                exp_name,
                {
                    "status": STATUS_NEEDS_RERUN,
                    "running_on": None,
                    "retry_count": (
                        experiments_by_name.get(exp_name, {}).get("retry_count", 0) or 0
                    )
                    + 1,
                    "error_info": {
                        "type": "ORPHAN_REAP",
                        "is_true_oom": False,
                        "message": f"Orphan training process reaped pid={pid}",
                        "peak_memory_mb": int(
                            experiments_by_name.get(exp_name, {}).get(
                                "peak_memory_mb", 0
                            )
                            or 0
                        ),
                        "failed_at": datetime.now().isoformat(),
                    },
                },
            )
            if ok:
                logger.log(f"Registry updated after orphan reap: {exp_name}")
        except Exception as e:
            logger.log(f"Registry sync after orphan reap failed ({exp_name}): {e}")

    for seen_pid in list(ORPHAN_TRAINING_SEEN.keys()):
        if seen_pid not in live_pids:
            ORPHAN_TRAINING_SEEN.pop(seen_pid, None)

    if killed:
        logger.log(f"Orphan training reaper killed {killed} process(es)")


def check_stale_locks(
    db: ExperimentsDB,
    logger,
    local_worker_id: Optional[str] = None,
    cluster_mgr: Optional[ClusterManager] = None,
):
    stale_results = db.check_stale_experiments(
        stale_sec=HEARTBEAT_STALE_SEC,
        caller_worker=local_worker_id,
        buddy_report_ttl_sec=SENTINEL_BUDDY_REPORT_TTL_SEC if SENTINEL_ENABLED else None,
    )
    for name, stale_worker in stale_results:
        logger.log(
            f"Resetting stale experiment {name} (worker {stale_worker} heartbeat missing)"
        )


def self_heal_heartbeat_worker_conflicts(
    db: ExperimentsDB,
    cluster_mgr: ClusterManager,
    logger,
    min_age_sec: int = 20,
):
    data = db.load()
    experiments = data.get("experiments", [])
    if not isinstance(experiments, list) or not experiments:
        return

    cluster_status = cluster_mgr.get_cluster_status(db)
    for exp in experiments:
        if not isinstance(exp, dict):
            continue
        if normalize_status(exp.get("status")) != STATUS_RUNNING:
            continue

        name = str(exp.get("name", "")).strip()
        if not name:
            continue
        running_on = exp.get("running_on") or {}
        registry_worker = str(running_on.get("worker", "")).strip()
        if not registry_worker:
            continue

        started_at = running_on.get("started_at")
        if isinstance(started_at, str) and started_at:
            try:
                started_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                age = (datetime.now(started_dt.tzinfo) - started_dt).total_seconds()
                if age < float(min_age_sec):
                    continue
            except Exception:
                pass

        workers_reporting: List[str] = []
        for worker_id, info in cluster_status.items():
            if str(info.get("status", "")).upper() != "ONLINE":
                continue
            running = info.get("running_experiments") or []
            if isinstance(running, str):
                running = [running]
            if not isinstance(running, list):
                continue
            normalized = {str(x).strip() for x in running if str(x).strip()}
            if name in normalized:
                workers_reporting.append(str(worker_id))

        if registry_worker in workers_reporting:
            continue

        observed_workers = [w for w in workers_reporting if w != registry_worker]
        if len(observed_workers) != 1:
            continue

        observed_worker = observed_workers[0]
        ok = db.heal_running_worker_owner(name, observed_worker)
        if ok:
            logger.log(
                f"Self-healed heartbeat worker conflict for {name}: "
                f"registry={registry_worker}, observed={observed_worker} "
                "-> updated RUNNING ownership"
            )


def check_zombie_processes(
    db: ExperimentsDB,
    worker_id: str,
    logger,
    protected_names: Optional[set[str]] = None,
):
    zombies = db.check_zombie_processes(worker_id, exclude_names=protected_names)
    for name, pid in zombies:
        logger.log(f"Zombie detected: {name} (PID {pid} dead)")


def process_remote_termination_requests(db: ExperimentsDB, worker_id: str, logger):
    requests = db.fetch_remote_termination_requests(worker_id)
    for req in requests:
        name = str(req.get("name") or "").strip()
        if not name:
            continue
        action = str(req.get("action") or "rerun").strip().lower()
        current_pid = req.get("pid")
        requested_pid = req.get("requested_pid")
        if not isinstance(current_pid, int) or current_pid <= 1:
            db.clear_remote_termination_request(name, worker_id)
            continue
        if (
            isinstance(requested_pid, int)
            and requested_pid > 1
            and requested_pid != current_pid
        ):
            db.clear_remote_termination_request(name, worker_id)
            continue

        killed = _kill_local_pid_tree(current_pid)
        if not killed:
            logger.log(
                f"Remote termination request failed kill: exp={name} pid={current_pid} action={action}"
            )
            continue

        if action == "kill":
            ok = db.kill_experiment(name)
        elif action == "freeze":
            ok = db.freeze_experiment(name)
        elif action == "start_now":
            ok = db.start_experiment_now(name)
        else:
            ok = db.rerun_experiment(name)
            if ok:
                removed = _clean_experiment_artifacts(name)
                logger.log(
                    f"Clean rerun reset artifacts for {name}: {len(removed)} removed"
                )
        db.clear_remote_termination_request(name, worker_id)
        logger.log(
            f"Processed remote termination: exp={name} pid={current_pid} action={action} ok={ok}"
        )
