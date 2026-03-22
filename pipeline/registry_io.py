#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared registry IO with NFS-safe, clock-skew-resistant locking.

Lock file content format:  hostname|pid|timestamp|nonce
Stale detection uses FILE CONTENT timestamp (not mtime) to avoid NFS clock skew.
Same-host locks: PID liveness check via os.kill(pid, 0).
Cross-host locks: content timestamp + lifetime_sec.
Belt-and-suspenders: future mtime guard as fallback.
"""

import json
import os
import socket
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

DEFAULT_REGISTRY = {"experiments": [], "archived": []}

# Hostname cached at import time (doesn't change during process lifetime)
_HOSTNAME = socket.gethostname()


# =============================================================================
# Lock Content Helpers
# =============================================================================


def _make_lock_content() -> str:
    """Generate lock file content: hostname|pid|timestamp|nonce."""
    nonce = os.urandom(4).hex()
    return f"{_HOSTNAME}|{os.getpid()}|{time.time()}|{nonce}"


def _parse_lock_content(lock_path: Path) -> Optional[Dict[str, Any]]:
    """Parse lock file content. Returns None if unparseable or missing."""
    try:
        with open(lock_path, "r") as f:
            content = f.read().strip()
        if not content:
            return None
        parts = content.split("|")
        if len(parts) >= 3:
            return {
                "hostname": parts[0],
                "pid": int(parts[1]),
                "timestamp": float(parts[2]),
                "nonce": parts[3] if len(parts) > 3 else "",
            }
        # Legacy format: just PID (from old registry_io.py)
        try:
            pid = int(content)
            return {"hostname": "", "pid": pid, "timestamp": 0.0, "nonce": ""}
        except ValueError:
            return None
    except (FileNotFoundError, IOError, ValueError):
        return None


def _is_lock_stale(lock_path: Path, lifetime_sec: int) -> bool:
    """Check if lock is stale using content timestamp + PID liveness.

    Strategy:
    1. Parse lock content → get hostname, pid, timestamp.
    2. Same host → os.kill(pid, 0). Dead PID = stale.
    3. Cross host → content timestamp age > lifetime_sec = stale.
    4. Fallback: unparseable content or future mtime → stale.
    """
    info = _parse_lock_content(lock_path)

    if info is None:
        # Unparseable or empty → treat as stale (safe to reclaim)
        return True

    now = time.time()

    # Legacy format (no hostname, timestamp=0): use mtime with future guard
    if not info["hostname"] and info["timestamp"] == 0.0:
        return _is_mtime_stale(lock_path, lifetime_sec)

    # Same host: PID liveness check (most reliable)
    if info["hostname"] == _HOSTNAME:
        try:
            os.kill(info["pid"], 0)
            return False  # Process is alive → not stale
        except ProcessLookupError:
            return True  # Process is dead → stale
        except PermissionError:
            return False  # Process exists but different user → not stale

    # Cross host: content timestamp age
    age = now - info["timestamp"]
    if age > lifetime_sec:
        return True
    # Negative age means their clock is ahead of ours, but within reason
    if age < -60:
        # Their clock is >60s ahead — suspicious but not necessarily stale.
        # Be conservative: don't break it yet.
        return False

    return False


def _is_mtime_stale(lock_path: Path, lifetime_sec: int) -> bool:
    """Fallback mtime-based stale check WITH future-time guard."""
    try:
        mtime = lock_path.stat().st_mtime
        now = time.time()
        # Future mtime guard: if mtime is >60s in the future, NFS clock skew
        if mtime > now + 60:
            return True
        return (now - mtime) > lifetime_sec
    except (FileNotFoundError, OSError):
        return False


# =============================================================================
# Lock Path
# =============================================================================


def _lock_path_for(registry_path: Path) -> Path:
    return registry_path.parent / "locks" / f"{registry_path.name}.lock"


# =============================================================================
# Registry Lock (NFS-safe)
# =============================================================================


@contextmanager
def registry_lock(registry_path: Path, timeout_sec: int = 15, stale_sec: int = 120):
    """NFS-safe registry lock using O_EXCL + content-based stale detection.

    Args:
        registry_path: Path to the registry JSON file.
        timeout_sec: Max seconds to wait for lock acquisition.
        stale_sec: Seconds after which a lock is considered stale (for cross-host).
                   Reduced from 600 to 120 — registry ops should be sub-second.
    """
    lock_path = _lock_path_for(registry_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()

    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, _make_lock_content().encode("utf-8"))
            os.close(fd)
            break
        except FileExistsError:
            # Lock exists — check if stale
            if _is_lock_stale(lock_path, stale_sec):
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    pass
                continue
            if time.time() - start > timeout_sec:
                # Include diagnostics in error message
                info = _parse_lock_content(lock_path)
                detail = ""
                if info:
                    age = time.time() - info["timestamp"] if info["timestamp"] else "?"
                    detail = (
                        f" (held by {info['hostname']}:{info['pid']}, age={age:.1f}s)"
                    )
                raise TimeoutError(
                    f"Registry lock timeout after {timeout_sec}s: {lock_path}{detail}"
                )
            time.sleep(0.1)

    try:
        yield
    finally:
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


# =============================================================================
# Registry Normalization
# =============================================================================


def _normalize_registry(data: Any) -> Dict[str, Any]:
    if isinstance(data, list):
        return {"experiments": data, "archived": []}
    if not isinstance(data, dict):
        return {"experiments": [], "archived": []}
    if "experiments" not in data:
        data["experiments"] = []
    if "archived" not in data:
        data["archived"] = []
    return data


# =============================================================================
# Registry IO
# =============================================================================


def _write_registry(registry_path: Path, data: Dict[str, Any]):
    fd, tmp_path = tempfile.mkstemp(
        dir=str(registry_path.parent),
        prefix=f".{registry_path.name}.",
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
        os.replace(tmp_path, registry_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass


def load_registry(registry_path: Path) -> Dict[str, Any]:
    if not registry_path.exists():
        return {"experiments": [], "archived": []}
    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _normalize_registry(data)
    except Exception:
        # Read must be side-effect free. In a multi-worker/NFS environment,
        # renaming or deleting the shared registry can break concurrent runners.
        return {"experiments": [], "archived": []}


def update_registry(
    registry_path: Path, updater: Callable[[Dict[str, Any]], Tuple[Dict[str, Any], Any]]
) -> Any:
    with registry_lock(registry_path):
        if not registry_path.exists():
            raise FileNotFoundError(f"Registry not found: {registry_path}")
        with open(registry_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data = _normalize_registry(data)
        data, result = updater(data)
        _write_registry(registry_path, data)
        return result


def save_registry(registry_path: Path, data: Dict[str, Any]):
    with registry_lock(registry_path):
        _write_registry(registry_path, data)


# =============================================================================
# Cleanup Utilities
# =============================================================================


def cleanup_orphan_files(registry_path: Path, max_age_sec: int = 3600):
    """Remove orphaned .tmp and .nfs files from registry directory.

    These accumulate due to:
    - .tmp: mkstemp files from killed processes
    - .nfs: NFS silly-rename when os.replace() overwrites a file still open elsewhere

    Args:
        registry_path: Path to the registry JSON file (cleanup happens in its parent dir).
        max_age_sec: Only remove files older than this (default: 1 hour).
    """
    parent = registry_path.parent
    now = time.time()
    removed = 0

    for pattern in [f".{registry_path.name}.*.tmp", ".nfs*"]:
        for orphan in parent.glob(pattern):
            try:
                stat = orphan.stat()
                mtime = stat.st_mtime
                # Future mtime guard (NFS clock skew)
                if mtime > now + 60:
                    age = max_age_sec + 1
                else:
                    age = now - mtime
                if age > max_age_sec:
                    orphan.unlink()
                    removed += 1
            except (FileNotFoundError, OSError):
                pass

    return removed
