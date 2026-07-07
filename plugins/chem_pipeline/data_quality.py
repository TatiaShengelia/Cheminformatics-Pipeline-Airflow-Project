"""Data-quality gate applied after clustering, before the optional stages.

Checks:
  - valid SMILES ratio (parseable by RDKit)
  - duplicate canonical SMILES ratio
  - missing-property ratio (property calc failures)
  - property values within plausible chemical ranges (MolWt, LogP)
  - non-empty result set

Returns a report dict plus an overall pass/fail so the DAG can decide whether
to gate the optional downstream stages and/or alert the team.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd
from rdkit import Chem


@dataclass
class DQResult:
    dataset_id: str
    passed: bool
    checks: dict = field(default_factory=dict)
    failures: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "passed": self.passed,
            "checks": self.checks,
            "failures": self.failures,
        }


def run_data_quality_checks(df: pd.DataFrame, dataset_id: str, cfg: dict) -> DQResult:
    checks: dict = {}
    failures: list[str] = []

    n_total = len(df)
    checks["n_molecules"] = n_total
    if n_total == 0:
        failures.append("No molecules produced (empty dataset)")
        return DQResult(dataset_id=dataset_id, passed=False, checks=checks, failures=failures)

    valid_mask = df["smiles"].apply(lambda s: Chem.MolFromSmiles(s) is not None)
    valid_ratio = valid_mask.mean()
    checks["valid_smiles_ratio"] = round(float(valid_ratio), 4)
    if valid_ratio < cfg["min_valid_smiles_ratio"]:
        failures.append(f"valid_smiles_ratio {valid_ratio:.2%} below threshold "
                         f"{cfg['min_valid_smiles_ratio']:.2%}")

    dup_ratio = 1 - (df["canonical_smiles"].nunique() / n_total) if "canonical_smiles" in df else 0.0
    checks["duplicate_ratio"] = round(float(dup_ratio), 4)
    if dup_ratio > cfg["max_duplicate_ratio"]:
        failures.append(f"duplicate_ratio {dup_ratio:.2%} above threshold {cfg['max_duplicate_ratio']:.2%}")

    for prop in ("mol_wt", "log_p"):
        if prop not in df:
            continue
        missing_ratio = df[prop].isna().mean()
        checks[f"missing_{prop}_ratio"] = round(float(missing_ratio), 4)
        if missing_ratio > cfg["max_missing_property_ratio"]:
            failures.append(f"missing_{prop}_ratio {missing_ratio:.2%} above threshold "
                             f"{cfg['max_missing_property_ratio']:.2%}")

    if "mol_wt" in df:
        lo, hi = cfg["molwt_range"]
        out_of_range = ((df["mol_wt"] < lo) | (df["mol_wt"] > hi)).mean()
        checks["mol_wt_out_of_range_ratio"] = round(float(out_of_range), 4)
        if out_of_range > cfg["max_missing_property_ratio"]:
            failures.append(f"{out_of_range:.2%} of molecules have MolWt outside [{lo}, {hi}]")

    if "log_p" in df:
        lo, hi = cfg["logp_range"]
        out_of_range = ((df["log_p"] < lo) | (df["log_p"] > hi)).mean()
        checks["log_p_out_of_range_ratio"] = round(float(out_of_range), 4)
        if out_of_range > cfg["max_missing_property_ratio"]:
            failures.append(f"{out_of_range:.2%} of molecules have LogP outside [{lo}, {hi}]")

    return DQResult(dataset_id=dataset_id, passed=(len(failures) == 0), checks=checks, failures=failures)
