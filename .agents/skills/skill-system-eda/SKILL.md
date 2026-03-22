---
name: skill-system-eda
description: Exploratory Data Analysis skill for CSV and parquet datasets with deterministic profiling, drift/anomaly scans, contract generation and validation, and optional memory writeback into skill-system-memory. The implementation is Polars-first (lazy scan for large files and early `--sample` head), includes high-cardinality guards for profile/importance/contract flows, and supports categorical correlation with Cramer's V. Use when building or reviewing tabular fraud/risk/data-quality workflows, profiling new datasets, checking leakage or drift, or saving/validating data contracts.
---

# Skill System EDA

Use `scripts/eda.py` for deterministic tabular analysis artifacts.

## Core Commands

```bash
python3 scripts/eda.py profile-dataset --input data.csv --target Class --output /tmp/eda
python3 scripts/eda.py distribution-report --input data.csv --target Class --profile /tmp/eda/profile.yaml
python3 scripts/eda.py correlation-matrix --input data.csv --target Class --profile /tmp/eda/profile.yaml
python3 scripts/eda.py anomaly-profiling --input data.csv --target Class --profile /tmp/eda/profile.yaml
python3 scripts/eda.py feature-importance-scan --input data.csv --target Class --profile /tmp/eda/profile.yaml
python3 scripts/eda.py leakage-detector --input data.csv --target Class --profile /tmp/eda/profile.yaml
python3 scripts/eda.py save-contract --profile /tmp/eda/profile.yaml --output /tmp/eda/contract.yaml
python3 scripts/eda.py validate-contract --input new_data.csv --contract /tmp/eda/contract.yaml
```

## Output Model

- `profile-dataset` creates `profile.yaml` and `report.md`
- later commands update `profile.yaml` and append sections to `report.md`
- `save-contract` emits `contract.yaml`
- `validate-contract` prints JSON `PASS` / `FAIL` with a violation list

## Analysis Rules

- Use Polars (not pandas) for data IO/aggregation/profiling flows.
- Keep sampling deterministic with lazy `.head(N)` when `--sample` is used.
- Treat `profile.yaml` as the machine-readable source of truth; `report.md` is the human-readable companion.
- Use Polars + numpy + scipy for profiling, shifts, correlations, KS tests, and Cramer's V.
- Use sklearn feature ranking only when available; otherwise keep tree-based importance explicitly skipped.
- Use lazy scan strategy for large CSV/parquet inputs (`scan_csv`/`scan_parquet`), with materialization delayed until needed.
- Apply high-cardinality guards: `>50` unique skips one-hot in feature importance, and profile truncates categorical columns (`>100` unique or `>50%` row cardinality) to top-20 values.

## Memory Integration

- By default, commands write a summary memory plus one memory per warning/critical finding.
- Prefer `skill-system-memory/scripts/mem.py store` when available.
- If memory writes fail or `EDA_DISABLE_MEM_PY=1` is set, write fallback payloads under `.memory/pending/`.
- Use `--no-memory` for deterministic tests or when no writeback is desired.

## Contract Lifecycle

- `save-contract` derives column requirements from `profile.yaml`.
- Numeric ranges use observed bounds for tiny datasets and profile-derived percentile bounds for larger datasets.
- Truncated categorical columns produce `cardinality_range` rules instead of `allowed_values`.
- `validate-contract` fails closed and returns machine-readable violations.

```skill-manifest
{
  "schema_version": "2.0",
  "id": "skill-system-eda",
  "version": "1.0.0",
  "capabilities": [
    "eda-profile",
    "eda-distribution",
    "eda-correlation",
    "eda-anomaly",
    "eda-feature-importance",
    "eda-leakage",
    "eda-contract-save",
    "eda-contract-validate"
  ],
  "effects": ["fs.read", "fs.write", "proc.exec"],
  "operations": {
    "profile-dataset": {
      "description": "Profile a CSV/parquet dataset and generate profile.yaml plus report.md.",
      "input": {
        "input": { "type": "string", "required": true },
        "target": { "type": "string", "required": false },
        "output": { "type": "string", "required": true },
        "sample": { "type": "integer", "required": false },
        "no_memory": { "type": "boolean", "required": false }
      },
      "output": {
        "description": "Artifact paths for the generated EDA profile",
        "fields": { "profile": "string", "report": "string" }
      },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "profile-dataset", "--input", "{input}", "--output", "{output}"]
      }
    },
    "distribution-report": {
      "description": "Append distribution and class-conditional analysis to an existing profile/report.",
      "input": {
        "input": { "type": "string", "required": true },
        "target": { "type": "string", "required": true },
        "profile": { "type": "string", "required": true }
      },
      "output": { "description": "Updated profile/report paths", "fields": { "profile": "string", "report": "string" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "distribution-report", "--input", "{input}", "--target", "{target}", "--profile", "{profile}"]
      }
    },
    "correlation-matrix": {
      "description": "Compute feature and target correlations and append them to profile/report.",
      "input": {
        "input": { "type": "string", "required": true },
        "target": { "type": "string", "required": false },
        "profile": { "type": "string", "required": true }
      },
      "output": { "description": "Updated profile/report paths", "fields": { "profile": "string", "report": "string" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "correlation-matrix", "--input", "{input}", "--profile", "{profile}"]
      }
    },
    "anomaly-profiling": {
      "description": "Compare class-conditional distributions and effect sizes.",
      "input": {
        "input": { "type": "string", "required": true },
        "target": { "type": "string", "required": true },
        "profile": { "type": "string", "required": true }
      },
      "output": { "description": "Updated profile/report paths", "fields": { "profile": "string", "report": "string" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "anomaly-profiling", "--input", "{input}", "--target", "{target}", "--profile", "{profile}"]
      }
    },
    "feature-importance-scan": {
      "description": "Rank features with mutual information and optional tree importances.",
      "input": {
        "input": { "type": "string", "required": true },
        "target": { "type": "string", "required": true },
        "profile": { "type": "string", "required": true }
      },
      "output": { "description": "Updated profile/report paths", "fields": { "profile": "string", "report": "string" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "feature-importance-scan", "--input", "{input}", "--target", "{target}", "--profile", "{profile}"]
      }
    },
    "leakage-detector": {
      "description": "Detect high-correlation, target-encoding, and temporal leakage indicators.",
      "input": {
        "input": { "type": "string", "required": true },
        "target": { "type": "string", "required": true },
        "profile": { "type": "string", "required": true }
      },
      "output": { "description": "Updated profile/report paths", "fields": { "profile": "string", "report": "string" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "leakage-detector", "--input", "{input}", "--target", "{target}", "--profile", "{profile}"]
      }
    },
    "save-contract": {
      "description": "Generate a data contract from a saved EDA profile.",
      "input": {
        "profile": { "type": "string", "required": true },
        "output": { "type": "string", "required": true }
      },
      "output": { "description": "Contract path", "fields": { "contract": "string" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "save-contract", "--profile", "{profile}", "--output", "{output}"]
      }
    },
    "validate-contract": {
      "description": "Validate a new dataset against a saved contract and emit PASS/FAIL JSON.",
      "input": {
        "input": { "type": "string", "required": true },
        "contract": { "type": "string", "required": true }
      },
      "output": { "description": "Validation status and violations", "fields": { "status": "string", "violations": "array" } },
      "entrypoints": {
        "unix": ["python3", "scripts/eda.py", "validate-contract", "--input", "{input}", "--contract", "{contract}"]
      }
    }
  },
  "stdout_contract": {
    "last_line_json": true
  }
}
```
