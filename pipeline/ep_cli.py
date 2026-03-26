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
    from control_plane import ControlPlaneService
except ModuleNotFoundError:
    from pipeline.cli_shared import add_common_args, emit_result, setup_logging
    from pipeline.control_plane import ControlPlaneService


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
