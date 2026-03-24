from unittest.mock import MagicMock, patch


def _mock_db(heartbeats, buddy_reports):
    db = MagicMock()
    db.get_cluster_heartbeats.return_value = heartbeats
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

    db = _mock_db(
        {
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
        },
        {},
    )
    db.cleanup_worker_heartbeats.return_value = 1

    status = cm.get_cluster_status(db)

    assert "SOTA" in status
    assert "oc-rerun4" not in status
    db.cleanup_worker_heartbeats.assert_called_once_with(["SOTA"])
