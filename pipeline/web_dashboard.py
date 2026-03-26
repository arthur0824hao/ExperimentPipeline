from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

try:
    from cluster import ClusterManager
    from control_plane import ControlPlaneService
    from db_registry import DBExperimentsDB
except ModuleNotFoundError:
    from pipeline.cluster import ClusterManager
    from pipeline.control_plane import ControlPlaneService
    from pipeline.db_registry import DBExperimentsDB


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DASHBOARD_HTML = STATIC_DIR / "dashboard.html"


class ClusterActionRequest(BaseModel):
    node_id: str
    action: str


def _load_preprocess_status() -> Dict[str, Any]:
    candidates = [
        BASE_DIR / "preprocess_status.json",
        BASE_DIR / "preprocess_progress.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload["source"] = path.name
                return payload
        except Exception:
            continue
    return {"status": "idle", "source": None}


def _run_cluster_action(
    action: str,
    node_id: str,
    cluster_mgr: ClusterManager,
    db: DBExperimentsDB,
) -> Dict[str, Any]:
    action = action.strip().lower()
    if action not in {"start", "stop", "restart", "disable", "enable"}:
        raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")

    if action == "disable":
        ok, msg = cluster_mgr.stop_node(node_id)
        if not ok:
            return {"ok": False, "message": msg}
        killed = db.kill_experiments_on_worker(node_id)
        db.disable_worker(node_id)
        return {
            "ok": True,
            "message": msg,
            "killed": int(killed),
            "worker_disabled": True,
        }

    if action == "enable":
        ok, msg = cluster_mgr.start_node(node_id)
        if ok:
            db.enable_worker(node_id)
        return {
            "ok": bool(ok),
            "message": msg,
            "worker_disabled": False if ok else db.is_worker_disabled(node_id),
        }

    if action == "restart":
        ok, msg = cluster_mgr.restart_node(node_id)
        killed = 0
        if ok:
            killed = int(db.kill_experiments_on_worker(node_id))
        return {"ok": bool(ok), "message": msg, "killed": killed}

    if action == "start":
        ok, msg = cluster_mgr.start_node(node_id)
        return {"ok": bool(ok), "message": msg}

    ok, msg = cluster_mgr.stop_node(node_id)
    return {"ok": bool(ok), "message": msg}


def create_app(
    db: Optional[DBExperimentsDB] = None,
    cluster_mgr: Optional[ClusterManager] = None,
) -> FastAPI:
    app = FastAPI(title="ExperimentPipeline Web Dashboard")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost",
            "http://127.0.0.1",
            "http://localhost:8501",
            "http://127.0.0.1:8501",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    app.state.db = db or DBExperimentsDB()
    app.state.cluster_mgr = cluster_mgr or ClusterManager()
    app.state.control_plane = ControlPlaneService(
        db=app.state.db, cluster_mgr=app.state.cluster_mgr,
    )

    @app.get("/")
    def dashboard_index() -> FileResponse:
        if not DASHBOARD_HTML.exists():
            raise HTTPException(status_code=404, detail="dashboard.html not found")
        return FileResponse(str(DASHBOARD_HTML))

    @app.get("/api/health")
    def api_health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/cluster")
    def api_cluster() -> Dict[str, Any]:
        result = app.state.control_plane.get_cluster_health()
        result["ts"] = int(time.time())
        return result

    @app.get("/api/experiments")
    def api_experiments(page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        return app.state.control_plane.list_experiments(
            page=max(1, int(page)),
            per_page=max(1, min(200, int(per_page))),
        )

    @app.get("/api/preprocess")
    def api_preprocess() -> Dict[str, Any]:
        return _load_preprocess_status()

    @app.post("/api/actions")
    def api_actions(req: ClusterActionRequest) -> Dict[str, Any]:
        result = _run_cluster_action(
            req.action,
            req.node_id,
            app.state.cluster_mgr,
            app.state.db,
        )
        result["node_id"] = req.node_id
        result["action"] = req.action
        return result

    return app


@dataclass
class WebServerHandle:
    thread: threading.Thread
    server: Any

    def stop(self, timeout: float = 5.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=timeout)


def start_web_server(
    port: int,
    db: DBExperimentsDB,
    cluster_mgr: ClusterManager,
    host: str = "127.0.0.1",
) -> WebServerHandle:
    import uvicorn

    app = create_app(db=db, cluster_mgr=cluster_mgr)
    config = uvicorn.Config(app=app, host=host, port=int(port), log_level="info")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, name="web-dashboard", daemon=True)
    thread.start()
    return WebServerHandle(thread=thread, server=server)


app = create_app()
