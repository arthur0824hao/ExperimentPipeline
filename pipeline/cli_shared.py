#!/usr/bin/env python3
"""Shared CLI helpers for Phase3 scripts."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from typing import Any, Mapping, MutableMapping, Sequence


SUCCESS = 0
GENERAL_ERROR = 1
INVALID_ARGS = 2
RUNTIME_ERROR = 3
EXTERNAL_DEP_ERROR = 4

EXIT_CODES = {
    "SUCCESS": SUCCESS,
    "GENERAL_ERROR": GENERAL_ERROR,
    "INVALID_ARGS": INVALID_ARGS,
    "RUNTIME_ERROR": RUNTIME_ERROR,
    "EXTERNAL_DEP_ERROR": EXTERNAL_DEP_ERROR,
}


def _stringify_command(command: str | Sequence[str] | None) -> str:
    if command is None:
        return ""
    return command if isinstance(command, str) else " ".join(command)


def _normalize_code(raw_code: Any) -> int:
    try:
        return int(raw_code) if int(raw_code) in EXIT_CODES.values() else GENERAL_ERROR
    except (TypeError, ValueError):
        return GENERAL_ERROR


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--verbose", "-v", action="count", default=0)
    parser.add_argument("--output", choices=("text", "json"), default="text")
    parser.add_argument("--quiet", action="store_true", default=False)
    return parser


def add_training_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--hidden-dim", type=int)
    parser.add_argument("--lr", type=float)
    parser.add_argument("--max-epochs", type=int)
    parser.add_argument("--gpu", type=int)
    parser.add_argument("--batch-size", type=int)
    return parser


def setup_logging(args: argparse.Namespace) -> int:
    if getattr(args, "quiet", False):
        level = logging.ERROR
    elif getattr(args, "debug", False):
        level = logging.DEBUG
    elif getattr(args, "verbose", 0) > 0:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.basicConfig(
        level=level, format="%(levelname)s:%(name)s:%(message)s", force=True
    )
    return level


def apply_training_args_to_env(
    args: argparse.Namespace,
    env: MutableMapping[str, str] | None = None,
) -> MutableMapping[str, str]:
    target = os.environ if env is None else env
    for key, value in {
        "HIDDEN_DIM": getattr(args, "hidden_dim", None),
        "LR": getattr(args, "lr", None),
        "MAX_EPOCHS": getattr(args, "max_epochs", None),
        "BATCH_SIZE": getattr(args, "batch_size", None),
        "GPU": getattr(args, "gpu", None),
    }.items():
        if value is not None:
            target[key] = str(value)
    if getattr(args, "gpu", None) is not None:
        target["CUDA_VISIBLE_DEVICES"] = str(args.gpu)
    return target


def format_error(
    code: int,
    command: str | Sequence[str] | None,
    message: str,
    hint: str | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "code": int(code),
            "command": _stringify_command(command),
            "message": str(message),
            "hint": hint,
        }
    }


def emit_result(
    args: argparse.Namespace,
    data: Any,
    status: str = "ok",
    stream: Any | None = None,
) -> str:
    mode = getattr(args, "output", "text")
    quiet = getattr(args, "quiet", False)
    output_stream = stream if stream is not None else sys.stdout
    if mode == "json":
        payload = {"status": status, "data": data}
        text = json.dumps(payload)
    else:
        if status == "ok":
            text = "" if data is None else str(data)
        elif isinstance(data, Mapping) and "message" in data:
            text = str(data["message"])
        elif isinstance(data, Mapping) and "error" in data:
            err = data["error"]
            text = str(err.get("message", "")) if isinstance(err, Mapping) else str(err)
        else:
            text = str(data)
    if not quiet:
        output_stream.write(text)
    return text


def run_with_cli(
    main_func: Any,
    parser: argparse.ArgumentParser,
    argv: Sequence[str] | None = None,
    output_stream: Any | None = None,
) -> int:
    argv_list = list(argv) if argv is not None else None
    command = _stringify_command([parser.prog] + (argv_list or []))
    try:
        args = parser.parse_args(argv_list)
        setup_logging(args)
        apply_training_args_to_env(args)
        result = main_func(args)
    except SystemExit as exc:
        code = _normalize_code(exc.code)
        if code == SUCCESS:
            return SUCCESS
        emit_result(
            argparse.Namespace(output="json", quiet=False),
            format_error(INVALID_ARGS, command, "Invalid arguments"),
            status="error",
            stream=output_stream,
        )
        return INVALID_ARGS
    except (ModuleNotFoundError, ImportError) as exc:
        if "args" in locals() and getattr(args, "debug", False):
            traceback.print_exc(file=output_stream or sys.stderr)
        emit_result(
            argparse.Namespace(output="json", quiet=False),
            format_error(EXTERNAL_DEP_ERROR, command, str(exc)),
            status="error",
            stream=output_stream,
        )
        return EXTERNAL_DEP_ERROR
    except Exception as exc:
        if "args" in locals() and getattr(args, "debug", False):
            traceback.print_exc(file=output_stream or sys.stderr)
        emit_result(
            argparse.Namespace(output="json", quiet=False),
            format_error(RUNTIME_ERROR, command, str(exc)),
            status="error",
            stream=output_stream,
        )
        return RUNTIME_ERROR

    if isinstance(result, tuple):
        code, payload = result
        normalized = _normalize_code(code)
    elif isinstance(result, int):
        normalized = _normalize_code(result)
        payload = None
    else:
        normalized = SUCCESS
        payload = result

    if normalized != SUCCESS:
        if not isinstance(payload, Mapping):
            payload = format_error(normalized, command, str(payload))
        elif "error" not in payload:
            payload = format_error(normalized, command, json.dumps(payload))
    emit_result(
        args,
        payload,
        status="ok" if normalized == SUCCESS else "error",
        stream=output_stream,
    )
    return normalized
