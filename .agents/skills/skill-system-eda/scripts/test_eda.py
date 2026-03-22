#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import polars as pl
import yaml


ROOT_DIR = Path(__file__).resolve().parents[3]
EDA_PATH = Path(__file__).with_name("eda.py")

spec = importlib.util.spec_from_file_location("eda_module", EDA_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("Failed to load eda.py module")
eda = importlib.util.module_from_spec(spec)
spec.loader.exec_module(eda)


class EdaCliTests(unittest.TestCase):
    def _make_dataset(self, root: Path, *, leaky: bool = False) -> Path:
        payload = {
            "amount": [100, 120, 500, 510, 115, 530],
            "score": [0.1, 0.2, 0.9, 0.95, 0.15, 0.85],
            "merchant": ["A", "A", "B", "B", "A", "B"],
            "timestamp": [
                "2026-03-01T00:00:00",
                "2026-03-02T00:00:00",
                "2026-03-03T00:00:00",
                "2026-03-04T00:00:00",
                "2026-03-05T00:00:00",
                "2026-03-06T00:00:00",
            ],
            "split": ["train", "train", "train", "test", "test", "test"],
            "Class": [0, 0, 1, 1, 0, 1],
        }
        if leaky:
            payload["leaky_feature"] = payload["Class"]
        df = pl.DataFrame(payload)
        path = root / "test.csv"
        df.write_csv(path)
        return path

    def _make_high_card_dataset(self, root: Path, rows: int = 563) -> Path:
        df = pl.DataFrame(
            {
                "amount": list(range(rows)),
                "merchant_id": [f"m_{i}" for i in range(rows)],
                "Class": [i % 2 for i in range(rows)],
            }
        )
        path = root / "high_card.csv"
        df.write_csv(path)
        return path

    def _make_cat_corr_dataset(self, root: Path) -> Path:
        df = pl.DataFrame(
            {
                "is_self_txn": ["Y", "N", "Y", "N", "Y", "N", "Y", "N"],
                "merchant": ["A", "B", "A", "B", "A", "B", "A", "B"],
                "region": ["N", "S", "N", "S", "N", "S", "N", "S"],
                "Class": [
                    "fraud",
                    "legit",
                    "fraud",
                    "legit",
                    "fraud",
                    "legit",
                    "fraud",
                    "legit",
                ],
            }
        )
        path = root / "cat_corr.csv"
        df.write_csv(path)
        return path

    def _run(
        self, *args: str, env: dict[str, str] | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(EDA_PATH), *args],
            cwd=ROOT_DIR,
            env=env,
            capture_output=True,
            text=True,
            check=check,
        )

    def _setup_profile(self, root: Path, *, leaky: bool = False) -> tuple[Path, Path]:
        dataset = self._make_dataset(root, leaky=leaky)
        output_dir = root / "eda"
        self._run(
            "profile-dataset",
            "--input",
            str(dataset),
            "--target",
            "Class",
            "--output",
            str(output_dir),
            "--no-memory",
        )
        return dataset, output_dir

    def test_profile_basic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            profile = yaml.safe_load((output_dir / "profile.yaml").read_text())
            report = (output_dir / "report.md").read_text(encoding="utf-8")
            self.assertEqual(Path(dataset).name, "test.csv")
            self.assertIn("columns", profile)
            self.assertIn("class_balance", profile["target"])
            self.assertIn("| column | dtype |", report)

    def test_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            profile_path = output_dir / "profile.yaml"
            self._run(
                "distribution-report",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            report = (output_dir / "report.md").read_text(encoding="utf-8")
            self.assertIn("distribution", profile)
            self.assertIn(
                "ks_test", profile["distribution"]["amount"]["class_conditional"]
            )
            self.assertIn("ks_pvalue", report)

    def test_correlation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            profile_path = output_dir / "profile.yaml"
            self._run(
                "correlation-matrix",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            self.assertIn("top_with_target", profile["correlations"])
            self.assertIn("high_inter_feature", profile["correlations"])

    def test_anomaly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            profile_path = output_dir / "profile.yaml"
            self._run(
                "anomaly-profiling",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            top = profile["anomaly"]["top_shifted_features"]
            self.assertTrue(top)
            self.assertIn("effect_size", top[0])

    def test_feature_importance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            profile_path = output_dir / "profile.yaml"
            self._run(
                "feature-importance-scan",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            ranking = profile["feature_importance"]["ranking"]
            self.assertTrue(ranking)
            self.assertIn("feature", ranking[0])

    def test_leakage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp), leaky=True)
            profile_path = output_dir / "profile.yaml"
            self._run(
                "leakage-detector",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            findings = profile["leakage"]["findings"]
            self.assertTrue(any(item["severity"] == "critical" for item in findings))

    def test_contract_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            contract_path = Path(tmp) / "contract.yaml"
            self._run(
                "save-contract",
                "--profile",
                str(output_dir / "profile.yaml"),
                "--output",
                str(contract_path),
            )
            result = self._run(
                "validate-contract",
                "--input",
                str(dataset),
                "--contract",
                str(contract_path),
            )
            self.assertEqual(json.loads(result.stdout)["status"], "PASS")

    def test_contract_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset, output_dir = self._setup_profile(Path(tmp))
            contract_path = Path(tmp) / "contract.yaml"
            self._run(
                "save-contract",
                "--profile",
                str(output_dir / "profile.yaml"),
                "--output",
                str(contract_path),
            )
            broken = pl.read_csv(dataset).with_columns(
                pl.when(pl.int_range(0, pl.len()) == 0)
                .then(pl.lit("Z"))
                .otherwise(pl.col("merchant"))
                .alias("merchant")
            )
            broken_path = Path(tmp) / "broken.csv"
            broken.write_csv(broken_path)
            result = self._run(
                "validate-contract",
                "--input",
                str(broken_path),
                "--contract",
                str(contract_path),
                check=False,
            )
            payload = json.loads(result.stdout)
            self.assertEqual(payload["status"], "FAIL")
            self.assertTrue(payload["violations"])

    def test_memory_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = self._make_dataset(root)
            output_dir = root / "eda"
            env = dict(os.environ)
            env["EDA_ROOT_DIR"] = str(root)
            env["EDA_DISABLE_MEM_PY"] = "1"
            self._run(
                "profile-dataset",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--output",
                str(output_dir),
                env=env,
            )
            pending = list((root / ".memory" / "pending").glob("*.json"))
            self.assertTrue(pending)
            self._run(
                "profile-dataset",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--output",
                str(root / "eda2"),
                "--no-memory",
                env=env,
            )
            second_pending = list((root / ".memory" / "pending").glob("*.json"))
            self.assertEqual(len(second_pending), len(pending))

    def test_large_file_lazy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dataset = self._make_dataset(Path(tmp))
            with mock.patch.object(eda, "LARGE_FILE_THRESHOLD_BYTES", 1):
                with mock.patch.object(
                    eda.pl, "scan_csv", wraps=eda.pl.scan_csv
                ) as scan_csv:
                    loaded = eda.read_dataset(dataset)
            self.assertTrue(scan_csv.called)
            self.assertEqual(loaded.height, 6)

    def test_high_cardinality_skip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = self._make_high_card_dataset(root)
            output_dir = root / "eda"
            self._run(
                "profile-dataset",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--output",
                str(output_dir),
                "--no-memory",
            )
            profile_path = output_dir / "profile.yaml"
            self._run(
                "feature-importance-scan",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            skipped = profile["feature_importance"]["skipped_high_cardinality"]
            ranking_features = [
                item["feature"] for item in profile["feature_importance"]["ranking"]
            ]
            self.assertIn("merchant_id", skipped)
            self.assertTrue(
                all(
                    not feature.startswith("merchant_id_")
                    for feature in ranking_features
                )
            )

    def test_profile_truncation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = self._make_high_card_dataset(root)
            output_dir = root / "eda"
            self._run(
                "profile-dataset",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--output",
                str(output_dir),
                "--no-memory",
            )
            profile = yaml.safe_load((output_dir / "profile.yaml").read_text())
            merchant = profile["columns"]["merchant_id"]
            self.assertTrue(merchant["truncated"])
            self.assertNotIn("allowed_values", merchant)
            self.assertEqual(len(merchant["top_values"]), 20)

    def test_cramers_v(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = self._make_cat_corr_dataset(root)
            output_dir = root / "eda"
            self._run(
                "profile-dataset",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--output",
                str(output_dir),
                "--no-memory",
            )
            profile_path = output_dir / "profile.yaml"
            self._run(
                "correlation-matrix",
                "--input",
                str(dataset),
                "--target",
                "Class",
                "--profile",
                str(profile_path),
                "--no-memory",
            )
            profile = yaml.safe_load(profile_path.read_text())
            report = (output_dir / "report.md").read_text(encoding="utf-8")
            target_corr = profile["correlations"]["top_with_target"]
            self.assertTrue(
                any(item.get("method") == "cramers_v" for item in target_corr)
            )
            self.assertIn("Categorical Correlations (Cramer's V)", report)


if __name__ == "__main__":
    unittest.main()
