#!/usr/bin/env python3

from pathlib import Path

from runtime_config import PHASE3_RUNTIME_CONFIG_FILE, get_runtime_section


def test_runtime_config_reads_main_project_phase3_runtime():
    section = get_runtime_section("phase3_graphsage_targeted")
    assert isinstance(section, dict)
    assert int(section.get("hidden_dim", -1)) == 56
    assert PHASE3_RUNTIME_CONFIG_FILE.as_posix().endswith("/configs/phase3_runtime.json")


def test_ep_duplicate_runtime_configs_removed():
    ep_config_dir = Path(__file__).resolve().parents[2] / "configs"
    assert not (ep_config_dir / "phase3_runtime.json").exists()
    assert not (ep_config_dir / "runtime.json").exists()
    assert not (ep_config_dir / "runtime.sample.json").exists()
