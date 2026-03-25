from unittest.mock import MagicMock, patch


def _mock_db(heartbeats, buddy_reports, *, filtered_heartbeats=None):
    db = MagicMock()
    db.get_cluster_heartbeats.return_value = heartbeats
    effective = filtered_heartbeats if filtered_heartbeats is not None else heartbeats
    db.get_filtered_cluster_heartbeats.return_value = effective
    db.get_worker_heartbeat.side_effect = lambda wid, *a, **kw: effective.get(wid, {})
    db.get_latest_buddy_reports.return_value = buddy_reports
    db.is_worker_disabled.return_value = False
    return db


def test_cluster_status_marks_online_when_heartbeat_fresh():
    from cluster import ClusterManager

    with patch.object(ClusterManager, "load_machines", return_value={"SOTA": {"host": "sota"}}):
        cm = ClusterManager()

    db = _mock_db(
        {
            "SOTA": {
                "last_seen_sec": 5,
                "gpus": [],
                "cpu": {},
                "running_jobs": 0,
                "running_experiments": [],
                "pid": 1,
            }
        },
        {
            "SOTA": {
                "reporter_id": "plusle",
                "target_process_alive": False,
                "target_db_reachable": False,
                "target_gpu_ok": False,
                "age_sec": 20,
            }
        },
    )

    status = cm.get_cluster_status(db)

    assert status["SOTA"]["status"] == "ONLINE"


def test_cluster_status_marks_db_degraded_when_heartbeat_stale_but_buddy_alive():
    from cluster import ClusterManager

    with patch.object(ClusterManager, "load_machines", return_value={"SOTA": {"host": "sota"}}):
        cm = ClusterManager()

    db = _mock_db(
        {
            "SOTA": {
                "last_seen_sec": 300,
                "gpus": [],
                "cpu": {},
                "running_jobs": 1,
                "running_experiments": ["EXP_A"],
                "pid": 2,
            }
        },
        {
            "SOTA": {
                "reporter_id": "plusle",
                "target_process_alive": True,
                "target_db_reachable": False,
                "target_gpu_ok": True,
                "age_sec": 12,
            }
        },
    )

    status = cm.get_cluster_status(db)

    assert status["SOTA"]["status"] == "DB_DEGRADED"
    assert status["SOTA"]["buddy_reporter"] == "plusle"


def test_cluster_status_marks_offline_when_stale_and_no_positive_buddy_report():
    from cluster import ClusterManager

    with patch.object(ClusterManager, "load_machines", return_value={"SOTA": {"host": "sota"}}):
        cm = ClusterManager()

    db = _mock_db(
        {
            "SOTA": {
                "last_seen_sec": 301,
                "gpus": [],
                "cpu": {},
                "running_jobs": 0,
                "running_experiments": [],
                "pid": 0,
            }
        },
        {"SOTA": {"target_process_alive": False, "age_sec": 10}},
    )

    status = cm.get_cluster_status(db)

    assert status["SOTA"]["status"] == "OFFLINE"


def test_cluster_status_ignores_non_whitelisted_workers_and_cleans_heartbeats():
    from cluster import ClusterManager

    with patch.object(ClusterManager, "load_machines", return_value={"SOTA": {"host": "sota"}}):
        cm = ClusterManager()

    raw = {
        "SOTA": {
            "last_seen_sec": 5,
            "gpus": [],
            "cpu": {},
            "running_jobs": 0,
            "running_experiments": [],
            "pid": 1,
        },
        "oc-rerun4": {
            "last_seen_sec": 5,
            "gpus": [],
            "cpu": {},
            "running_jobs": 1,
            "running_experiments": ["EXP_GHOST"],
            "pid": 2,
        },
    }
    filtered = {k: v for k, v in raw.items() if k == "SOTA"}

    db = _mock_db(raw, {}, filtered_heartbeats=filtered)
    db.cleanup_worker_heartbeats.return_value = 1

    status = cm.get_cluster_status(db)

    assert "SOTA" in status
    assert "oc-rerun4" not in status
    db.cleanup_worker_heartbeats.assert_called_once_with(["SOTA"])


def test_network_health_uses_filtered_heartbeats_only():
    from cluster import ClusterManager

    with patch.object(ClusterManager, "load_machines", return_value={"SOTA": {"host": "sota"}}):
        cm = ClusterManager()

    raw = {
        "SOTA": {"last_seen_sec": 5},
        "oc-rerun4": {"last_seen_sec": 1},
    }
    filtered = {"SOTA": {"last_seen_sec": 5}}
    db = _mock_db(raw, {}, filtered_heartbeats=filtered)

    with patch.object(cm, "_check_node_connectivity", return_value={"ok": True, "latency_ms": 10, "error": None, "error_kind": None, "host": "sota", "ssh_port": None}):
        health = cm.get_network_health(db)

    assert set(health["nodes"].keys()) == {"SOTA"}
    db.get_filtered_cluster_heartbeats.assert_called_once_with(["SOTA"], fail_closed=True)


def test_wait_for_heartbeat_resume_uses_single_worker_accessor():
    from cluster import ClusterManager

    with patch.object(ClusterManager, "load_machines", return_value={"SOTA": {"host": "sota"}}):
        cm = ClusterManager()

    fresh = {"SOTA": {"last_seen_sec": 5}}
    db = _mock_db({}, {}, filtered_heartbeats=fresh)

    ok, last_seen = cm._wait_for_heartbeat_resume("SOTA", db, timeout_sec=2)

    assert ok is True
    assert last_seen == 5
    db.get_worker_heartbeat.assert_called_with("SOTA", ["SOTA"], fail_closed=True)
