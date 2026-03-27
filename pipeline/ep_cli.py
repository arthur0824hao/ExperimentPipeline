#!/usr/bin/env python3
"""Agent CLI v1 for ExperimentPipeline control plane.

Provides machine-readable (--output json) and human-readable access to
experiment status, cluster health, and summary queries.

Usage:
    python3 pipeline/ep_cli.py status --output json
    python3 pipeline/ep_cli.py experiments --output json
    python3 pipeline/ep_cli.py experiment <name> --output json
    python3 pipeline/ep_cli.py cluster --output json
"""
from __future__ import annotations

import argparse
import sys
from typing import Sequence

try:
    from cli_shared import add_common_args, emit_result, setup_logging
    from compare import compare_experiments
    from control_plane import ControlPlaneService
    from run_manifest import build_manifest, build_manifest_batch
except ModuleNotFoundError:
    from pipeline.cli_shared import add_common_args, emit_result, setup_logging
    from pipeline.compare import compare_experiments
    from pipeline.control_plane import ControlPlaneService
    from pipeline.run_manifest import build_manifest, build_manifest_batch


DBExperimentsDB = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ep-cli",
        description="ExperimentPipeline Agent CLI v1",
    )
    add_common_args(parser)
    sub = parser.add_subparsers(dest="command")

    status = sub.add_parser("status", help="Overall status summary")
    add_common_args(status)

    exp_list = sub.add_parser("experiments", help="List experiments")
    add_common_args(exp_list)
    exp_list.add_argument("--page", type=int, default=1)
    exp_list.add_argument("--per-page", type=int, default=50)

    exp_detail = sub.add_parser("experiment", help="Single experiment detail")
    add_common_args(exp_detail)
    exp_detail.add_argument("name", help="Experiment name")

    cluster = sub.add_parser("cluster", help="Cluster health summary")
    add_common_args(cluster)

    manifest_one = sub.add_parser("manifest", help="Run manifest for one experiment")
    add_common_args(manifest_one)
    manifest_one.add_argument("name", help="Experiment name")

    manifest_all = sub.add_parser("manifests", help="Run manifests for all experiments")
    add_common_args(manifest_all)

    cmp = sub.add_parser("compare", help="Compare two experiments")
    add_common_args(cmp)
    cmp.add_argument("name_a", help="First experiment name")
    cmp.add_argument("name_b", help="Second experiment name")

    return parser


def _run(args: argparse.Namespace) -> int:
    setup_logging(args)
    svc = ControlPlaneService()
    command = args.command

    if command == "status":
        data = svc.get_status_summary()
    elif command == "experiments":
        data = svc.list_experiments(
            page=args.page,
            per_page=getattr(args, "per_page", 50),
        )
    elif command == "experiment":
        data = svc.get_experiment(args.name)
        if data is None:
            emit_result(
                args,
                {"error": {"message": f"Experiment '{args.name}' not found"}},
                status="error",
            )
            return 1
    elif command == "cluster":
        data = svc.get_cluster_health()
    elif command == "manifest":
        global DBExperimentsDB
        if DBExperimentsDB is None:
            try:
                from db_registry import DBExperimentsDB as _DBExperimentsDB
            except ModuleNotFoundError:
                from pipeline.db_registry import DBExperimentsDB as _DBExperimentsDB
            DBExperimentsDB = _DBExperimentsDB
        db = DBExperimentsDB()
        data = build_manifest(db, args.name)
        if data is None:
            emit_result(
                args,
                {"error": {"message": f"Experiment '{args.name}' not found"}},
                status="error",
            )
            return 1
    elif command == "manifests":
        if DBExperimentsDB is None:
            try:
                from db_registry import DBExperimentsDB as _DBExperimentsDB
            except ModuleNotFoundError:
                from pipeline.db_registry import DBExperimentsDB as _DBExperimentsDB
            DBExperimentsDB = _DBExperimentsDB
        db = DBExperimentsDB()
        data = build_manifest_batch(db)
    elif command == "compare":
        if DBExperimentsDB is None:
            try:
                from db_registry import DBExperimentsDB as _DBExperimentsDB
            except ModuleNotFoundError:
                from pipeline.db_registry import DBExperimentsDB as _DBExperimentsDB
            DBExperimentsDB = _DBExperimentsDB
        db = DBExperimentsDB()
        data = compare_experiments(db, args.name_a, args.name_b)
        if data is None:
            emit_result(
                args,
                {"error": {"message": "One or both experiments not found"}},
                status="error",
            )
            return 1
    else:
        emit_result(
            args,
            {"error": {"message": "No command specified. Use --help."}},
            status="error",
        )
        return 1

    emit_result(args, data, status="ok")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _run(args)


if __name__ == "__main__":
    sys.exit(main())
