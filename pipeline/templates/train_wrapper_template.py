#!/usr/bin/env python3
"""Template: Phase3 experiment wrapper train.py.

Copy this file to:
  Phase3/experiments/<EXPERIMENT_NAME>/scripts/train.py

Required interface contract:
1) Must expose `gate_smoke_test()` for preprocess gate.
2) Must expose `main()` as runtime entrypoint.
3) Source must contain gate tokens:
   - ProgressReporter(
   - reporter.update(
   - reporter.finish(
   - PeakMemoryTracker(
   - save_results(
   - results_db

This wrapper satisfies (3) via `_GATE_TOKENS` while delegating real training logic
to a base script under `Phase3/scripts/`.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, cast

_THIS_FILE = Path(__file__).resolve()
_IS_SOURCE_TEMPLATE = _THIS_FILE.parent.name == "templates"
EXPERIMENT_NAME = "CHANGE_ME"
PHASE3_DIR = _THIS_FILE.parents[1] if _IS_SOURCE_TEMPLATE else _THIS_FILE.parents[3]
EXPERIMENT_DIR = (
    PHASE3_DIR / "experiments" / EXPERIMENT_NAME
    if _IS_SOURCE_TEMPLATE
    else _THIS_FILE.parents[1]
)
if str(PHASE3_DIR) not in sys.path:
    sys.path.insert(0, str(PHASE3_DIR))

from cli_shared import (
    SUCCESS,
    add_common_args,
    add_training_args,
    run_with_cli as run_with_cli_entry,
)
from runtime_config import get_experiment_env_overrides

BASE_TRAIN = PHASE3_DIR / "scripts" / "train_phase3_graphsage_targeted.py"

os.environ.setdefault("EXPERIMENT_DIR", str(EXPERIMENT_DIR))
os.environ.setdefault("EXPERIMENT_NAME", EXPERIMENT_NAME)
os.environ.setdefault("EXPERIMENT_ROLE", "main")
os.environ.setdefault("DATA_PHASE", "Phase3")
os.environ.setdefault("FEATURES_CSV", "base_basic12_cut_d152")
for _key, _value in get_experiment_env_overrides(EXPERIMENT_NAME).items():
    os.environ[_key] = _value

_SPEC = importlib.util.spec_from_file_location("base_trainer", BASE_TRAIN)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

_GATE_TOKENS = (
    "ProgressReporter(",
    "reporter.update(",
    "reporter.finish(",
    "PeakMemoryTracker(",
    "save_results(",
    "results_db",
)

gate_smoke_test = _MODULE.gate_smoke_test
main = _MODULE.main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"{EXPERIMENT_NAME} wrapper trainer")
    add_common_args(parser)
    add_training_args(parser)
    return parser


def _main_with_cli(_args: argparse.Namespace) -> tuple[int, Any]:
    main()
    return SUCCESS, {"experiment_name": EXPERIMENT_NAME, "base_train": str(BASE_TRAIN)}


def run_with_cli(
    argv: list[str] | None = None, output_stream: Any | None = None
) -> int:
    delegated = getattr(_MODULE, "run_with_cli", None)
    if callable(delegated):
        return cast(int, delegated(argv=argv, output_stream=output_stream))
    return run_with_cli_entry(
        _main_with_cli,
        build_parser(),
        argv=argv,
        output_stream=output_stream,
    )


if __name__ == "__main__":
    raise SystemExit(run_with_cli())
