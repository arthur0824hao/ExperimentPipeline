#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
import yaml
from scipy import stats

RandomForestClassifier: Any = None
RandomForestRegressor: Any = None
mutual_info_classif: Any = None
mutual_info_regression: Any = None
try:
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.feature_selection import mutual_info_classif, mutual_info_regression

    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


SEED = 0
LARGE_FILE_THRESHOLD_BYTES = 64 * 1024 * 1024
HIGH_CARD_IMPORTANCE_THRESHOLD = 50
HIGH_CARD_PROFILE_UNIQUE_THRESHOLD = 100
HIGH_CARD_PROFILE_RATIO_THRESHOLD = 0.5
HIGH_CARD_PROFILE_TOP_VALUES = 20
NORMAL_PROFILE_TOP_VALUES = 5
ALLOWED_VALUES_LIMIT = 100
CATEGORY_CORR_MAX_CARDINALITY = 50

NUMERIC_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
    pl.Decimal,
}


def project_root() -> Path:
    override = os.environ.get("EDA_ROOT_DIR", "").strip()
    if override:
        return Path(override).resolve()
    return Path(__file__).resolve().parents[3]


def memory_pending_dir() -> Path:
    path = project_root() / ".memory" / "pending"
    path.mkdir(parents=True, exist_ok=True)
    return path


def memory_script_path() -> Path:
    return project_root() / "skills" / "skill-system-memory" / "scripts" / "mem.py"


def timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    return dtype in NUMERIC_DTYPES


def is_datetime_dtype(dtype: pl.DataType) -> bool:
    return dtype in {pl.Date, pl.Datetime, pl.Time}


def eda_dtype(series: pl.Series) -> str:
    if series.dtype == pl.Boolean:
        return "boolean"
    if is_numeric_dtype(series.dtype):
        return "numeric"
    if is_datetime_dtype(series.dtype):
        return "datetime"
    return "categorical"


def _should_use_lazy(input_path: Path, sample: int | None) -> bool:
    if sample is not None and sample > 0:
        return True
    try:
        return input_path.stat().st_size >= LARGE_FILE_THRESHOLD_BYTES
    except OSError:
        return False


def _collect_lazy(lf: pl.LazyFrame, sample: int | None) -> pl.DataFrame:
    if sample is not None and sample > 0:
        return lf.head(sample).collect()
    return lf.collect()


def read_dataset(input_path: Path, sample: int | None = None) -> pl.DataFrame:
    suffix = input_path.suffix.lower()
    use_lazy = _should_use_lazy(input_path, sample)

    if suffix == ".csv":
        if use_lazy:
            return _collect_lazy(pl.scan_csv(input_path), sample)
        if sample is not None and sample > 0:
            return pl.read_csv(input_path, n_rows=sample)
        return pl.read_csv(input_path)

    if suffix in {".parquet", ".pq"}:
        if use_lazy:
            return _collect_lazy(pl.scan_parquet(input_path), sample)
        if sample is not None and sample > 0:
            return pl.read_parquet(input_path).head(sample)
        return pl.read_parquet(input_path)

    raise ValueError(f"Unsupported input format: {input_path}")


def render_table(headers: list[str], rows: list[list[Any]]) -> str:
    header = "| " + " | ".join(headers) + " |"
    sep = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return "\n".join([header, sep, *body])


def ensure_output_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def artifact_paths(
    output_dir: Path | None, profile_path: Path | None
) -> tuple[Path, Path]:
    if profile_path is not None:
        profile_path = profile_path.resolve()
        report_path = profile_path.with_name("report.md")
        return profile_path, report_path
    if output_dir is None:
        output_dir = Path("eda-output")
    output_dir = ensure_output_dir(output_dir.resolve())
    return output_dir / "profile.yaml", output_dir / "report.md"


def load_profile(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid profile: {path}")
    return data


def save_profile(path: Path, profile: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(profile, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


def save_report(path: Path, sections: list[str]) -> None:
    path.write_text("\n\n".join(sections).strip() + "\n", encoding="utf-8")


def append_report(path: Path, section: str) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    content = existing.rstrip()
    if content:
        content += "\n\n"
    content += section.strip() + "\n"
    path.write_text(content, encoding="utf-8")


def _round_or_none(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    return round(float(value), 6)


def numeric_summary(series: pl.Series) -> dict[str, float | None]:
    casted = series.cast(pl.Float64, strict=False).drop_nulls()
    if casted.len() == 0:
        return {
            key: None
            for key in [
                "min",
                "max",
                "mean",
                "std",
                "p01",
                "p25",
                "p50",
                "p75",
                "p95",
                "p99",
            ]
        }
    return {
        "min": _round_or_none(casted.min()),
        "max": _round_or_none(casted.max()),
        "mean": _round_or_none(casted.mean()),
        "std": _round_or_none(casted.std(ddof=0)),
        "p01": _round_or_none(casted.quantile(0.01)),
        "p25": _round_or_none(casted.quantile(0.25)),
        "p50": _round_or_none(casted.quantile(0.5)),
        "p75": _round_or_none(casted.quantile(0.75)),
        "p95": _round_or_none(casted.quantile(0.95)),
        "p99": _round_or_none(casted.quantile(0.99)),
    }


def categorical_summary(
    series: pl.Series, top_n: int = NORMAL_PROFILE_TOP_VALUES
) -> list[dict[str, Any]]:
    normalized = series.cast(pl.Utf8, strict=False).fill_null("<NULL>")
    counts = (
        pl.DataFrame({"value": normalized})
        .group_by("value")
        .len()
        .sort("len", descending=True)
        .head(top_n)
    )
    total = max(series.len(), 1)
    return [
        {
            "value": str(row["value"]),
            "count": int(row["len"]),
            "pct": round(float(row["len"]) / total, 6),
        }
        for row in counts.to_dicts()
    ]


def _categorical_allowed_values(series: pl.Series) -> list[str]:
    values = series.cast(pl.Utf8, strict=False).drop_nulls().unique().to_list()
    values = [str(v) for v in values]
    values.sort()
    return values[:ALLOWED_VALUES_LIMIT]


def profile_dataframe(
    df: pl.DataFrame, dataset_name: str, target: str | None
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    row_count = int(df.height)
    profile: dict[str, Any] = {
        "dataset": {
            "name": dataset_name,
            "rows": row_count,
            "columns": int(len(df.columns)),
            "generated_at": timestamp(),
        },
        "columns": {},
        "findings": [],
    }
    findings: list[dict[str, Any]] = []
    rows_for_report: list[list[Any]] = []
    for column in df.columns:
        series = df.get_column(column)
        dtype = eda_dtype(series)
        missing = int(series.null_count())
        missing_pct = round(float(missing) / max(row_count, 1), 6)
        unique = int(series.drop_nulls().n_unique())
        cardinality = round(float(unique) / max(row_count, 1), 6)
        info: dict[str, Any] = {
            "dtype": dtype,
            "missing": missing,
            "missing_pct": missing_pct,
            "unique": unique,
            "cardinality": cardinality,
        }
        if dtype == "numeric":
            info["numeric_summary"] = numeric_summary(series)
        else:
            is_high_card = (
                unique > HIGH_CARD_PROFILE_UNIQUE_THRESHOLD
                or cardinality > HIGH_CARD_PROFILE_RATIO_THRESHOLD
            )
            top_n = (
                HIGH_CARD_PROFILE_TOP_VALUES
                if is_high_card
                else NORMAL_PROFILE_TOP_VALUES
            )
            info["top_values"] = categorical_summary(series, top_n=top_n)
            if is_high_card:
                info["truncated"] = True
            else:
                info["allowed_values"] = _categorical_allowed_values(series)
        profile["columns"][column] = info
        rows_for_report.append(
            [column, dtype, missing, missing_pct, unique, cardinality]
        )
        if missing_pct >= 0.5:
            findings.append(
                {
                    "severity": "warning",
                    "category": "missingness",
                    "feature": column,
                    "message": f"{column} has high missing rate ({missing_pct:.2%})",
                }
            )

    target_report = ""
    if target and target in df.columns:
        counts = (
            df.select(
                pl.col(target)
                .cast(pl.Utf8, strict=False)
                .fill_null("<NULL>")
                .alias("_target")
            )
            .group_by("_target")
            .len()
            .sort("_target")
        )
        ratios = {
            str(row["_target"]): round(float(row["len"]) / max(row_count, 1), 6)
            for row in counts.to_dicts()
        }
        minority_pct = min(ratios.values()) if ratios else 0.0
        alert = None
        if minority_pct < 0.05:
            alert = "critical"
        elif minority_pct < 0.1:
            alert = "warning"
        profile["target"] = {
            "name": target,
            "class_balance": {
                "ratios": ratios,
                "minority_pct": round(minority_pct, 6),
                "alert": alert,
            },
        }
        if alert:
            findings.append(
                {
                    "severity": alert,
                    "category": "class_balance",
                    "feature": target,
                    "message": f"Target {target} minority_pct={minority_pct:.2%}",
                }
            )
        target_report = render_table(
            ["class", "ratio"],
            [[name, ratio] for name, ratio in ratios.items()],
        )

    profile["findings"] = findings
    report = [
        "# Dataset Profile",
        f"- generated_at: {profile['dataset']['generated_at']}",
        f"- rows: {profile['dataset']['rows']}",
        f"- columns: {profile['dataset']['columns']}",
        "",
        "## Schema",
        render_table(
            ["column", "dtype", "missing", "missing_pct", "unique", "cardinality"],
            rows_for_report,
        ),
    ]
    if target_report:
        report.extend(["", "## Target Class Balance", target_report])
    return profile, findings, "\n".join(report)


def load_or_create_profile(
    input_path: Path,
    target: str | None,
    profile_path: Path | None,
    output_dir: Path | None,
    sample: int | None,
) -> tuple[pl.DataFrame, dict[str, Any], Path, Path]:
    df = read_dataset(input_path, sample)
    profile_file, report_file = artifact_paths(output_dir, profile_path)
    if profile_file.exists():
        profile = load_profile(profile_file)
    else:
        profile, findings, report = profile_dataframe(df, input_path.stem, target)
        save_profile(profile_file, profile)
        save_report(report_file, [report])
        maybe_store_memories(
            input_path.stem,
            findings,
            f"Profiled dataset {input_path.name}",
            no_memory=True,
        )
    return df, profile, profile_file, report_file


def class_conditional_stats(
    df: pl.DataFrame, target: str, column: str
) -> dict[str, Any]:
    if target not in df.columns or column not in df.columns:
        return {}
    groups = df.select([target, column]).drop_nulls()
    if groups.is_empty():
        return {}
    labels = groups.get_column(target).unique().to_list()
    labels = sorted(labels, key=lambda value: str(value))
    if len(labels) != 2:
        return {}

    result: dict[str, Any] = {"by_class": {}}
    samples: list[np.ndarray] = []
    for label in labels:
        values = (
            groups.filter(pl.col(target) == label)
            .get_column(column)
            .cast(pl.Float64, strict=False)
            .drop_nulls()
            .to_numpy()
        )
        if values.size == 0:
            continue
        result["by_class"][str(label)] = {
            "mean": round(float(np.mean(values)), 6),
            "std": round(float(np.std(values, ddof=0)), 6),
        }
        samples.append(values)
    if len(samples) == 2 and all(sample.size > 0 for sample in samples):
        ks: Any = stats.ks_2samp(samples[0], samples[1])
        result["ks_test"] = {
            "statistic": round(float(ks.statistic), 6),
            "pvalue": round(float(ks.pvalue), 6),
        }
    return result


def command_distribution_report(
    df: pl.DataFrame, profile: dict[str, Any], target: str | None
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    distribution: dict[str, Any] = {}
    findings: list[dict[str, Any]] = []
    rows: list[list[Any]] = []
    for column in df.columns:
        info = profile["columns"][column]
        entry: dict[str, Any] = {}
        if info["dtype"] == "numeric":
            series = df.get_column(column).cast(pl.Float64, strict=False).drop_nulls()
            values = series.to_numpy()
            if values.size > 0:
                entry["skewness"] = (
                    round(float(stats.skew(values, bias=False)), 6)
                    if values.size > 2
                    else 0.0
                )
                entry["kurtosis"] = (
                    round(float(stats.kurtosis(values, bias=False)), 6)
                    if values.size > 3
                    else 0.0
                )
                if target and column != target:
                    conditional = class_conditional_stats(df, target, column)
                    if conditional:
                        entry["class_conditional"] = conditional
                        ks_test = conditional.get("ks_test")
                        if ks_test and float(ks_test["pvalue"]) < 0.05:
                            findings.append(
                                {
                                    "severity": "warning",
                                    "category": "distribution_shift",
                                    "feature": column,
                                    "message": f"{column} differs significantly across target classes (KS p={ks_test['pvalue']})",
                                }
                            )
                rows.append(
                    [
                        column,
                        entry.get("skewness"),
                        entry.get("kurtosis"),
                        entry.get("class_conditional", {})
                        .get("ks_test", {})
                        .get("pvalue"),
                    ]
                )
        else:
            entry["value_counts"] = categorical_summary(df.get_column(column))
        distribution[column] = entry
    profile["distribution"] = distribution
    report = "\n".join(
        [
            "## Distribution Report",
            render_table(
                ["feature", "skewness", "kurtosis", "ks_pvalue"],
                rows or [["(none)", "", "", ""]],
            ),
        ]
    )
    return profile, findings, report


def _pearson(df: pl.DataFrame, left: str, right: str) -> float:
    pair = df.select(
        [
            pl.col(left).cast(pl.Float64, strict=False).alias("_left"),
            pl.col(right).cast(pl.Float64, strict=False).alias("_right"),
        ]
    ).drop_nulls()
    if pair.height < 2:
        return 0.0
    value = pair.select(pl.corr("_left", "_right")).item()
    if value is None:
        return 0.0
    value_f = float(value)
    if np.isnan(value_f):
        return 0.0
    return abs(value_f)


def _cramers_v(df: pl.DataFrame, left: str, right: str) -> float | None:
    pair = df.select(
        [
            pl.col(left).cast(pl.Utf8, strict=False).alias("_left"),
            pl.col(right).cast(pl.Utf8, strict=False).alias("_right"),
        ]
    ).drop_nulls()
    if pair.is_empty():
        return None
    left_card = int(pair.get_column("_left").n_unique())
    right_card = int(pair.get_column("_right").n_unique())
    if left_card < 2 or right_card < 2:
        return 0.0
    if (
        left_card > CATEGORY_CORR_MAX_CARDINALITY
        or right_card > CATEGORY_CORR_MAX_CARDINALITY
    ):
        return None

    counts = pair.group_by(["_left", "_right"]).len()
    pivot = counts.pivot(
        values="len", index="_left", on="_right", aggregate_function="sum"
    ).fill_null(0)
    matrix = pivot.drop("_left").to_numpy()
    if matrix.size == 0:
        return 0.0
    total = float(matrix.sum())
    if total <= 0:
        return 0.0
    rows, cols = matrix.shape
    denom = min(rows - 1, cols - 1)
    if denom <= 0:
        return 0.0
    contingency: Any = stats.chi2_contingency(matrix, correction=False)
    chi2 = float(contingency[0])
    return float(np.sqrt((chi2 / total) / denom))


def command_correlation_matrix(
    df: pl.DataFrame, profile: dict[str, Any], target: str | None, top_n: int = 10
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    findings: list[dict[str, Any]] = []
    column_info = profile.get("columns", {})
    numeric_cols = [
        col for col, info in column_info.items() if info.get("dtype") == "numeric"
    ]
    categorical_cols = [
        col
        for col, info in column_info.items()
        if info.get("dtype") in {"categorical", "boolean"}
    ]

    inter_pairs: list[dict[str, Any]] = []
    for i, left in enumerate(numeric_cols):
        for right in numeric_cols[i + 1 :]:
            value = _pearson(df, left, right)
            inter_pairs.append(
                {
                    "left": left,
                    "right": right,
                    "correlation": round(value, 6),
                    "method": "pearson",
                }
            )
            if value > 0.95:
                findings.append(
                    {
                        "severity": "warning",
                        "category": "high_correlation",
                        "feature": f"{left}:{right}",
                        "message": f"{left} and {right} are highly correlated ({value:.3f})",
                    }
                )

    for i, left in enumerate(categorical_cols):
        left_card = int(column_info.get(left, {}).get("unique", 0))
        if left_card > CATEGORY_CORR_MAX_CARDINALITY:
            continue
        for right in categorical_cols[i + 1 :]:
            right_card = int(column_info.get(right, {}).get("unique", 0))
            if right_card > CATEGORY_CORR_MAX_CARDINALITY:
                continue
            value = _cramers_v(df, left, right)
            if value is None:
                continue
            inter_pairs.append(
                {
                    "left": left,
                    "right": right,
                    "correlation": round(abs(value), 6),
                    "method": "cramers_v",
                }
            )

    inter_pairs.sort(key=lambda item: item["correlation"], reverse=True)

    target_corr: list[dict[str, Any]] = []
    if target and target in df.columns:
        target_dtype = column_info.get(target, {}).get("dtype")
        for column in df.columns:
            if column == target:
                continue
            col_dtype = column_info.get(column, {}).get("dtype")
            if target_dtype == "numeric" and col_dtype == "numeric":
                corr = _pearson(df, column, target)
                target_corr.append(
                    {
                        "feature": column,
                        "correlation": round(corr, 6),
                        "method": "pearson",
                    }
                )
                continue

            if target_dtype in {"categorical", "boolean"} and col_dtype in {
                "categorical",
                "boolean",
            }:
                target_card = int(column_info.get(target, {}).get("unique", 0))
                col_card = int(column_info.get(column, {}).get("unique", 0))
                if (
                    target_card <= CATEGORY_CORR_MAX_CARDINALITY
                    and col_card <= CATEGORY_CORR_MAX_CARDINALITY
                ):
                    corr = _cramers_v(df, column, target)
                    if corr is not None:
                        target_corr.append(
                            {
                                "feature": column,
                                "correlation": round(abs(corr), 6),
                                "method": "cramers_v",
                            }
                        )

        target_corr.sort(key=lambda item: item["correlation"], reverse=True)

    profile["correlations"] = {
        "top_with_target": target_corr[:top_n],
        "high_inter_feature": inter_pairs[:top_n],
    }

    categorical_rows = [
        [item["left"], item["right"], item["correlation"], item["method"]]
        for item in inter_pairs
        if item["method"] == "cramers_v"
    ][:top_n]
    report = "\n".join(
        [
            "## Correlation Matrix",
            "### Top with Target",
            render_table(
                ["feature", "correlation", "method"],
                [
                    [
                        item["feature"],
                        item["correlation"],
                        item.get("method", "pearson"),
                    ]
                    for item in target_corr[:top_n]
                ]
                or [["(none)", "", ""]],
            ),
            "",
            "### High Inter-Feature Correlations",
            render_table(
                ["left", "right", "correlation", "method"],
                [
                    [
                        item["left"],
                        item["right"],
                        item["correlation"],
                        item.get("method", "pearson"),
                    ]
                    for item in inter_pairs[:top_n]
                ]
                or [["(none)", "", "", ""]],
            ),
            "",
            "### Categorical Correlations (Cramer's V)",
            render_table(
                ["left", "right", "correlation", "method"],
                categorical_rows or [["(none)", "", "", ""]],
            ),
        ]
    )
    return profile, findings, report


def cohen_d(a: np.ndarray, b: np.ndarray) -> float:
    if a.size < 2 or b.size < 2:
        return 0.0
    a_var = np.var(a, ddof=1)
    b_var = np.var(b, ddof=1)
    pooled = np.sqrt(
        ((a.size - 1) * a_var + (b.size - 1) * b_var) / max(a.size + b.size - 2, 1)
    )
    if pooled == 0:
        return 0.0
    return float((np.mean(a) - np.mean(b)) / pooled)


def command_anomaly_profiling(
    df: pl.DataFrame, profile: dict[str, Any], target: str, top_n: int = 10
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    findings: list[dict[str, Any]] = []
    shifts: list[dict[str, Any]] = []
    if target not in df.columns:
        profile["anomaly"] = {"top_shifted_features": []}
        return profile, findings, "## Anomaly Profiling\n(no binary target available)"

    labels = df.get_column(target).drop_nulls().unique().to_list()
    labels = sorted(labels, key=lambda value: str(value))
    if len(labels) != 2:
        profile["anomaly"] = {"top_shifted_features": []}
        return profile, findings, "## Anomaly Profiling\n(no binary target available)"
    left_label, right_label = labels[0], labels[1]

    numeric_columns = [
        col
        for col, info in profile.get("columns", {}).items()
        if info.get("dtype") == "numeric" and col != target
    ]
    for column in numeric_columns:
        left = (
            df.filter(pl.col(target) == left_label)
            .get_column(column)
            .cast(pl.Float64, strict=False)
            .drop_nulls()
            .to_numpy()
        )
        right = (
            df.filter(pl.col(target) == right_label)
            .get_column(column)
            .cast(pl.Float64, strict=False)
            .drop_nulls()
            .to_numpy()
        )
        effect = abs(cohen_d(left, right))
        shifts.append({"feature": column, "effect_size": round(effect, 6)})
        if effect > 1.0:
            findings.append(
                {
                    "severity": "warning",
                    "category": "anomaly_shift",
                    "feature": column,
                    "message": f"{column} has large class shift (Cohen's d={effect:.3f})",
                }
            )

    shifts.sort(key=lambda item: item["effect_size"], reverse=True)
    profile["anomaly"] = {"top_shifted_features": shifts[:top_n]}
    report = "\n".join(
        [
            "## Anomaly Profiling",
            render_table(
                ["feature", "effect_size"],
                [[item["feature"], item["effect_size"]] for item in shifts[:top_n]]
                or [["(none)", ""]],
            ),
        ]
    )
    return profile, findings, report


def _build_feature_matrix(
    model_df: pl.DataFrame,
    profile: dict[str, Any],
    target: str,
) -> tuple[np.ndarray, list[str], list[str]]:
    feature_names: list[str] = []
    skipped_high_cardinality: list[str] = []
    columns: list[np.ndarray] = []

    for column in model_df.columns:
        if column == target:
            continue
        info = profile.get("columns", {}).get(column, {})
        dtype = info.get("dtype")
        if dtype == "numeric":
            arr = (
                model_df.get_column(column)
                .cast(pl.Float64, strict=False)
                .fill_null(strategy="mean")
                .fill_null(0.0)
                .to_numpy()
            )
            columns.append(arr.reshape(-1, 1))
            feature_names.append(column)
            continue

        if dtype == "boolean":
            arr = (
                model_df.get_column(column)
                .cast(pl.Int8, strict=False)
                .fill_null(0)
                .cast(pl.Float64)
                .to_numpy()
            )
            columns.append(arr.reshape(-1, 1))
            feature_names.append(column)
            continue

        cardinality = int(info.get("unique", 0))
        if cardinality > HIGH_CARD_IMPORTANCE_THRESHOLD:
            skipped_high_cardinality.append(column)
            continue

        dummies = model_df.select(
            pl.col(column).cast(pl.Utf8, strict=False).fill_null("<NULL>").alias(column)
        ).to_dummies(columns=[column])
        for dummy_col in dummies.columns:
            arr = dummies.get_column(dummy_col).cast(pl.Float64).to_numpy()
            columns.append(arr.reshape(-1, 1))
            feature_names.append(dummy_col)

    if not columns:
        return np.zeros((model_df.height, 0)), feature_names, skipped_high_cardinality
    matrix = np.hstack(columns)
    return matrix, feature_names, skipped_high_cardinality


def command_feature_importance(
    df: pl.DataFrame, profile: dict[str, Any], target: str, top_n: int = 20
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    findings: list[dict[str, Any]] = []
    if target not in df.columns:
        profile["feature_importance"] = {"ranking": []}
        return profile, findings, "## Feature Importance\n(target not found)"

    model_df = df.filter(pl.col(target).is_not_null())
    if model_df.is_empty():
        profile["feature_importance"] = {"ranking": []}
        return (
            profile,
            findings,
            "## Feature Importance\n(no rows after dropping target nulls)",
        )

    features, feature_names, skipped_high_cardinality = _build_feature_matrix(
        model_df, profile, target
    )

    ranking: list[dict[str, Any]] = []
    y_series = model_df.get_column(target)
    is_regression = (
        eda_dtype(y_series) == "numeric" and y_series.drop_nulls().n_unique() > 10
    )

    if features.shape[1] > 0:
        if is_regression:
            y = y_series.cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
            mi = (
                mutual_info_regression(features, y, random_state=SEED)
                if HAS_SKLEARN
                else np.zeros(features.shape[1])
            )
            tree_values = None
            if HAS_SKLEARN:
                model = RandomForestRegressor(n_estimators=100, random_state=SEED)
                model.fit(features, y)
                tree_values = model.feature_importances_
        else:
            y_text = y_series.cast(pl.Utf8, strict=False).fill_null("<NULL>").to_numpy()
            _, y = np.unique(y_text, return_inverse=True)
            mi = (
                mutual_info_classif(features, y, random_state=SEED)
                if HAS_SKLEARN
                else np.zeros(features.shape[1])
            )
            tree_values = None
            if HAS_SKLEARN:
                model = RandomForestClassifier(n_estimators=100, random_state=SEED)
                model.fit(features, y)
                tree_values = model.feature_importances_

        for idx, column in enumerate(feature_names):
            entry = {"feature": column, "mutual_information": round(float(mi[idx]), 6)}
            if tree_values is not None:
                entry["tree_importance"] = round(float(tree_values[idx]), 6)
            ranking.append(entry)

        ranking.sort(
            key=lambda item: (
                item.get("tree_importance", item["mutual_information"]),
                item["mutual_information"],
            ),
            reverse=True,
        )

    if skipped_high_cardinality:
        findings.append(
            {
                "severity": "warning",
                "category": "high_cardinality",
                "feature": ",".join(skipped_high_cardinality),
                "message": f"Skipped one-hot encoding for high-cardinality features (> {HIGH_CARD_IMPORTANCE_THRESHOLD} unique)",
            }
        )

    profile["feature_importance"] = {
        "ranking": ranking[:top_n],
        "sklearn_available": HAS_SKLEARN,
        "skipped_high_cardinality": skipped_high_cardinality,
        "high_cardinality_threshold": HIGH_CARD_IMPORTANCE_THRESHOLD,
    }
    report = "\n".join(
        [
            "## Feature Importance",
            render_table(
                ["feature", "mutual_information", "tree_importance"],
                [
                    [
                        item["feature"],
                        item["mutual_information"],
                        item.get("tree_importance", "skipped"),
                    ]
                    for item in ranking[:top_n]
                ]
                or [["(none)", "", ""]],
            ),
            f"- skipped_high_cardinality: {','.join(skipped_high_cardinality) if skipped_high_cardinality else 'none'}",
        ]
    )
    return profile, findings, report


def command_leakage_detector(
    df: pl.DataFrame, profile: dict[str, Any], target: str
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    findings: list[dict[str, Any]] = []
    if target not in df.columns:
        profile["leakage"] = {"findings": []}
        return profile, findings, "## Leakage Detector\n(target not found)"

    target_unique = int(df.get_column(target).drop_nulls().n_unique())
    for column in df.columns:
        if column == target:
            continue
        corr = _pearson(df, column, target)
        if corr > 0.95:
            findings.append(
                {
                    "severity": "critical",
                    "category": "leakage",
                    "feature": column,
                    "message": f"{column} correlation with {target} is {corr:.3f}",
                }
            )

        feature_unique = int(df.get_column(column).drop_nulls().n_unique())
        if feature_unique == target_unique and target_unique > 0:
            findings.append(
                {
                    "severity": "warning",
                    "category": "target_encoding",
                    "feature": column,
                    "message": f"{column} unique cardinality matches target cardinality",
                }
            )

    split_col = next(
        (col for col in df.columns if col.lower() in {"split", "dataset_split"}), None
    )
    time_cols = [
        col
        for col in df.columns
        if "time" in col.lower() or col.lower().endswith("_at")
    ]
    if split_col and time_cols:
        for time_col in time_cols:
            parsed = df.select(
                [
                    pl.col(split_col)
                    .cast(pl.Utf8, strict=False)
                    .str.to_lowercase()
                    .alias("_split"),
                    pl.col(time_col)
                    .cast(pl.Utf8, strict=False)
                    .str.strptime(pl.Datetime, strict=False)
                    .alias("_time"),
                ]
            ).drop_nulls()
            if parsed.is_empty():
                continue
            train_max = (
                parsed.filter(pl.col("_split") == "train")
                .select(pl.col("_time").max())
                .item()
            )
            test_min = (
                parsed.filter(pl.col("_split") == "test")
                .select(pl.col("_time").min())
                .item()
            )
            if train_max is not None and test_min is not None and test_min <= train_max:
                findings.append(
                    {
                        "severity": "warning",
                        "category": "temporal_leakage",
                        "feature": time_col,
                        "message": f"{time_col} test timestamps overlap train range",
                    }
                )

    profile["leakage"] = {"findings": findings}
    report = "\n".join(
        [
            "## Leakage Detector",
            render_table(
                ["severity", "category", "feature", "message"],
                [
                    [f["severity"], f["category"], f["feature"], f["message"]]
                    for f in findings
                ]
                or [["PASS", "none", "", "No leakage findings"]],
            ),
        ]
    )
    return profile, findings, report


def command_save_contract(profile: dict[str, Any]) -> dict[str, Any]:
    contract: dict[str, Any] = {
        "dataset": profile.get("dataset", {}),
        "required_columns": sorted(profile.get("columns", {}).keys()),
        "columns": {},
    }
    for column, info in profile.get("columns", {}).items():
        entry: dict[str, Any] = {
            "dtype": info.get("dtype"),
            "nullable": bool(info.get("missing", 0) > 0),
        }
        if info.get("dtype") == "numeric":
            summary = info.get("numeric_summary") or {}
            observed_min = summary.get("min")
            observed_max = summary.get("max")
            if observed_min is not None and observed_max is not None:
                span = observed_max - observed_min
                tolerance = span * 0.01 if span > 0 else 1.0
            else:
                tolerance = 0
            entry["numeric_range"] = {
                "min": round(observed_min - tolerance, 6)
                if observed_min is not None
                else None,
                "max": round(observed_max + tolerance, 6)
                if observed_max is not None
                else None,
            }
        else:
            if info.get("truncated"):
                observed_unique = int(info.get("unique", 0))
                entry["cardinality_range"] = {
                    "min": observed_unique,
                    "max": observed_unique,
                }
            else:
                entry["allowed_values"] = list(
                    info.get("allowed_values")
                    or [item["value"] for item in (info.get("top_values") or [])]
                )
        contract["columns"][column] = entry
    if profile.get("target", {}).get("class_balance"):
        minority = profile["target"]["class_balance"]["minority_pct"]
        contract["class_balance"] = {
            "target": profile["target"]["name"],
            "minority_pct_range": [
                round(max(0.0, minority - 0.05), 6),
                round(min(1.0, minority + 0.05), 6),
            ],
        }
    return contract


def command_validate_contract(
    df: pl.DataFrame, contract: dict[str, Any]
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    required_columns = contract.get("required_columns", [])
    for column in required_columns:
        if column not in df.columns:
            violations.append(
                {
                    "type": "missing_column",
                    "column": column,
                    "message": f"Missing column {column}",
                }
            )

    for column, rules in contract.get("columns", {}).items():
        if column not in df.columns:
            continue
        series = df.get_column(column)
        dtype = eda_dtype(series)
        if dtype != rules.get("dtype"):
            violations.append(
                {
                    "type": "dtype_mismatch",
                    "column": column,
                    "message": f"Expected {rules.get('dtype')} got {dtype}",
                }
            )
        if not rules.get("nullable", True) and int(series.null_count()) > 0:
            violations.append(
                {
                    "type": "nullability",
                    "column": column,
                    "message": f"{column} should be non-nullable",
                }
            )
        if rules.get("dtype") == "numeric" and "numeric_range" in rules:
            values = series.cast(pl.Float64, strict=False).drop_nulls()
            if values.len() > 0:
                min_bound = rules["numeric_range"].get("min")
                max_bound = rules["numeric_range"].get("max")
                observed_min = float(cast(Any, values.min()))
                observed_max = float(cast(Any, values.max()))
                if min_bound is not None and observed_min < float(min_bound):
                    violations.append(
                        {
                            "type": "range",
                            "column": column,
                            "message": f"{column} below min range",
                        }
                    )
                if max_bound is not None and observed_max > float(max_bound):
                    violations.append(
                        {
                            "type": "range",
                            "column": column,
                            "message": f"{column} above max range",
                        }
                    )
        if rules.get("dtype") == "categorical" and rules.get("allowed_values"):
            observed = set(
                str(value)
                for value in series.cast(pl.Utf8, strict=False)
                .drop_nulls()
                .unique()
                .to_list()
            )
            allowed = set(rules["allowed_values"])
            if not observed.issubset(allowed):
                violations.append(
                    {
                        "type": "allowed_values",
                        "column": column,
                        "message": f"{column} has values outside contract",
                    }
                )
        if rules.get("dtype") == "categorical" and rules.get("cardinality_range"):
            observed = int(series.cast(pl.Utf8, strict=False).drop_nulls().n_unique())
            card_min = int(rules["cardinality_range"].get("min", 0))
            card_max = int(rules["cardinality_range"].get("max", 0))
            if observed < card_min or observed > card_max:
                violations.append(
                    {
                        "type": "cardinality_range",
                        "column": column,
                        "message": f"{column} cardinality {observed} outside range [{card_min}, {card_max}]",
                    }
                )

    class_balance = contract.get("class_balance")
    if class_balance and class_balance.get("target") in df.columns:
        target_col = class_balance["target"]
        counts = (
            df.select(
                pl.col(target_col)
                .cast(pl.Utf8, strict=False)
                .fill_null("<NULL>")
                .alias("_target")
            )
            .group_by("_target")
            .len()
        )
        total = max(
            float(counts.get_column("len").sum()) if counts.height > 0 else 0.0, 1.0
        )
        minority = (
            float(cast(Any, (counts.get_column("len") / total).min()))
            if counts.height > 0
            else 0.0
        )
        lower, upper = class_balance["minority_pct_range"]
        if minority < lower or minority > upper:
            violations.append(
                {
                    "type": "class_balance",
                    "column": target_col,
                    "message": f"minority_pct {minority:.4f} outside range [{lower}, {upper}]",
                }
            )
    return {"status": "PASS" if not violations else "FAIL", "violations": violations}


def pending_memory_payload(
    dataset_name: str,
    title: str,
    tags: list[str],
    importance: float,
    content: str,
    metadata: dict[str, Any],
) -> Path:
    pending = (
        memory_pending_dir() / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    )
    pending.write_text(
        json.dumps(
            {
                "memory_type": "semantic",
                "category": "eda-finding",
                "title": title,
                "tags_csv": ",".join(tags),
                "importance": importance,
                "content": content,
                "metadata": metadata,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return pending


def maybe_store_memories(
    dataset_name: str, findings: list[dict[str, Any]], summary: str, no_memory: bool
) -> None:
    if no_memory:
        return
    disable_mem_py = os.environ.get("EDA_DISABLE_MEM_PY", "") == "1"
    mem_py = memory_script_path()

    def store_one(
        title: str, severity: str, category: str, content: str, metadata: dict[str, Any]
    ) -> None:
        tags = [dataset_name, category]
        importance = (
            0.9 if severity == "critical" else 0.6 if severity == "warning" else 0.5
        )
        if not disable_mem_py and mem_py.exists():
            result = subprocess.run(
                [
                    sys.executable,
                    str(mem_py),
                    "store",
                    "semantic",
                    "eda-finding",
                    title,
                    ",".join(tags),
                    str(importance),
                    "--content",
                    content,
                ],
                cwd=project_root(),
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return
        pending_memory_payload(dataset_name, title, tags, importance, content, metadata)

    store_one(
        f"EDA Summary: {dataset_name}",
        "summary",
        "summary",
        summary,
        {"dataset": dataset_name, "kind": "summary", "generated_at": timestamp()},
    )
    for finding in findings:
        store_one(
            f"EDA {finding['severity'].upper()}: {dataset_name}::{finding['feature']}",
            finding["severity"],
            finding["category"],
            finding["message"],
            {"dataset": dataset_name, **finding, "generated_at": timestamp()},
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="eda.py", description="Exploratory Data Analysis skill CLI"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_common(p: argparse.ArgumentParser, target_required: bool = False) -> None:
        p.add_argument("--input", required=True)
        p.add_argument("--target", required=target_required)
        p.add_argument("--profile")
        p.add_argument("--output")
        p.add_argument("--sample", type=int)
        p.add_argument("--no-memory", action="store_true")

    p_profile = sub.add_parser("profile-dataset")
    add_common(p_profile)

    p_distribution = sub.add_parser("distribution-report")
    add_common(p_distribution, target_required=True)

    p_corr = sub.add_parser("correlation-matrix")
    add_common(p_corr)
    p_corr.add_argument("--top", type=int, default=10)

    p_anomaly = sub.add_parser("anomaly-profiling")
    add_common(p_anomaly, target_required=True)
    p_anomaly.add_argument("--top", type=int, default=10)

    p_importance = sub.add_parser("feature-importance-scan")
    add_common(p_importance, target_required=True)
    p_importance.add_argument("--top", type=int, default=20)

    p_leakage = sub.add_parser("leakage-detector")
    add_common(p_leakage, target_required=True)

    p_save = sub.add_parser("save-contract")
    p_save.add_argument("--profile", required=True)
    p_save.add_argument("--output", required=True)

    p_validate = sub.add_parser("validate-contract")
    p_validate.add_argument("--input", required=True)
    p_validate.add_argument("--contract", required=True)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.cmd == "profile-dataset":
        output_dir = (
            ensure_output_dir(Path(args.output).resolve()) if args.output else None
        )
        df = read_dataset(Path(args.input), args.sample)
        profile, findings, report = profile_dataframe(
            df, Path(args.input).stem, args.target
        )
        profile_file, report_file = artifact_paths(
            output_dir, Path(args.profile).resolve() if args.profile else None
        )
        save_profile(profile_file, profile)
        save_report(report_file, [report])
        maybe_store_memories(
            Path(args.input).stem,
            findings,
            f"Profile generated for {Path(args.input).name}",
            args.no_memory,
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "profile": str(profile_file),
                    "report": str(report_file),
                }
            )
        )
        return 0

    if args.cmd == "save-contract":
        profile = load_profile(Path(args.profile).resolve())
        contract = command_save_contract(profile)
        output = Path(args.output).resolve()
        output.write_text(
            yaml.safe_dump(contract, sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        print(json.dumps({"status": "ok", "contract": str(output)}))
        return 0

    if args.cmd == "validate-contract":
        df = read_dataset(Path(args.input).resolve())
        contract = yaml.safe_load(Path(args.contract).read_text(encoding="utf-8")) or {}
        result = command_validate_contract(df, contract)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["status"] == "PASS" else 1

    input_path = Path(args.input).resolve()
    profile_arg = Path(args.profile).resolve() if args.profile else None
    output_dir = (
        Path(args.output).resolve()
        if args.output
        else (profile_arg.parent if profile_arg else None)
    )
    df, profile, profile_file, report_file = load_or_create_profile(
        input_path, getattr(args, "target", None), profile_arg, output_dir, args.sample
    )

    if args.cmd == "distribution-report":
        profile, findings, report = command_distribution_report(
            df, profile, args.target
        )
    elif args.cmd == "correlation-matrix":
        profile, findings, report = command_correlation_matrix(
            df, profile, getattr(args, "target", None), args.top
        )
    elif args.cmd == "anomaly-profiling":
        profile, findings, report = command_anomaly_profiling(
            df, profile, args.target, args.top
        )
    elif args.cmd == "feature-importance-scan":
        profile, findings, report = command_feature_importance(
            df, profile, args.target, args.top
        )
    elif args.cmd == "leakage-detector":
        profile, findings, report = command_leakage_detector(df, profile, args.target)
    else:
        raise ValueError(f"Unsupported command: {args.cmd}")

    profile.setdefault("findings", [])
    profile["findings"].extend(findings)
    save_profile(profile_file, profile)
    append_report(report_file, report)
    maybe_store_memories(
        input_path.stem,
        findings,
        f"{args.cmd} completed for {input_path.name}",
        args.no_memory,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "profile": str(profile_file),
                "report": str(report_file),
                "findings": len(findings),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
