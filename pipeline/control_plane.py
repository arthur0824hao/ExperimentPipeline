#!/usr/bin/env python3
"""Shared control-plane query/service layer.

Provides a single entry point for queries about experiments, cluster health,
and status summaries. Both the TUI and CLI use this layer instead of
directly accessing db_registry / cluster internals.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

try:
    from db_registry import DBExperimentsDB
    from cluster import ClusterManager
except ModuleNotFoundError:
    from pipeline.db_registry import DBExperimentsDB
    from pipeline.cluster import ClusterManager


class ControlPlaneService:
    """Read-only query facade over DBExperimentsDB and ClusterManager."""

    def __init__(
        self,
        db: Optional[DBExperimentsDB] = None,
        cluster_mgr: Optional[ClusterManager] = None,
    ) -> None:
        self.db = db or DBExperimentsDB()
        self.cluster_mgr = cluster_mgr or ClusterManager()

    # -- Experiments queries ------------------------------------------------

    def list_experiments(self, *, page: int = 1, per_page: int = 50) -> Dict[str, Any]:
        """Return paginated experiment list (same shape as /api/experiments)."""
        rows = self.db.load_all_for_panel() or []
        total = len(rows)
        page = max(1, page)
        per_page = max(1, min(200, per_page))
        start = (page - 1) * per_page
        end = start + per_page
        return {
            "items": rows[start:end],
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": max(1, (total + per_page - 1) // per_page),
        }

    def get_experiment(self, name: str) -> Optional[Dict[str, Any]]:
        """Return detail dict for a single experiment, or None."""
        return self.db.get_experiment(name)

    # -- Cluster queries ----------------------------------------------------

    def get_cluster_health(self) -> Dict[str, Any]:
        """Return cluster health summary with worker statuses."""
        status = self.cluster_mgr.get_cluster_status(self.db)
        for node_id, info in status.items():
            disabled = bool(self.db.is_worker_disabled(node_id))
            info["worker_disabled"] = disabled
            if disabled:
                info["status"] = "DISABLED"
        return {"cluster": status}

    # -- Aggregate status ---------------------------------------------------

    def get_status_summary(self) -> Dict[str, Any]:
        """Return a high-level status snapshot."""
        snapshot = self.db.load()
        experiments = snapshot.get("experiments", [])
        completed = snapshot.get("completed", [])

        by_status: Dict[str, int] = {}
        for exp in experiments:
            st = str(exp.get("status", "UNKNOWN")).upper()
            by_status[st] = by_status.get(st, 0) + 1

        cluster = self.cluster_mgr.get_cluster_status(self.db)
        online = sum(1 for v in cluster.values() if v.get("status") == "ONLINE")
        total_workers = len(cluster)

        return {
            "active_experiments": len(experiments),
            "completed_experiments": len(completed),
            "by_status": by_status,
            "workers_online": online,
            "workers_total": total_workers,
        }
