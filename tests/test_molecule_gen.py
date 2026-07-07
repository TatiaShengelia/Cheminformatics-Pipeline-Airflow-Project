from unittest.mock import MagicMock

import pandas as pd

from chem_pipeline.molecule_gen import generate_molecules
from chem_pipeline.properties import add_properties
from chem_pipeline.s3_utils import DatasetPair, filter_new_or_overwrite


def test_generate_molecules_simple_scaffold():
    scaffold_df = pd.DataFrame({"id": [1], "smiles": ["CC*"]})
    r_groups_df = pd.DataFrame({"id": [1, 1], "smiles": ["*O", "*N"]})

    result = generate_molecules(scaffold_df, r_groups_df, dataset_id="test_ds")

    assert len(result) == 2
    assert set(result["dataset_id"]) == {"test_ds"}
    assert all(result["smiles"].str.len() > 0)


def test_add_properties_produces_expected_columns():
    df = pd.DataFrame({"smiles": ["CCO", "invalid_smiles_zzz"]})
    result = add_properties(df)

    assert "mol_wt" in result.columns
    assert "log_p" in result.columns
    # valid molecule should have a computed mol_wt
    assert result.loc[0, "mol_wt"] is not None
    # invalid SMILES should degrade gracefully instead of raising
    assert pd.isna(result.loc[1, "mol_wt"])


def _hook_with_no_existing_outputs():
    hook = MagicMock()
    hook.check_for_key.return_value = False
    return hook


def test_filter_new_or_overwrite_respects_watermark():
    pairs = {
        "old_ds": DatasetPair("old_ds", "in/old_ds_scaffolds.csv", "in/old_ds_r_groups.csv",
                               last_modified="2026-01-01T00:00:00+00:00"),
        "new_ds": DatasetPair("new_ds", "in/new_ds_scaffolds.csv", "in/new_ds_r_groups.csv",
                               last_modified="2026-06-01T00:00:00+00:00"),
    }
    hook = _hook_with_no_existing_outputs()

    selected = filter_new_or_overwrite(
        hook, pairs, bucket="b", output_prefix="processed/",
        last_run_ts="2026-03-01T00:00:00+00:00", overwrite=False,
    )

    assert [p.dataset_id for p in selected] == ["new_ds"]


def test_filter_new_or_overwrite_true_ignores_watermark():
    pairs = {
        "old_ds": DatasetPair("old_ds", "in/old_ds_scaffolds.csv", "in/old_ds_r_groups.csv",
                               last_modified="2026-01-01T00:00:00+00:00"),
    }
    hook = _hook_with_no_existing_outputs()

    selected = filter_new_or_overwrite(
        hook, pairs, bucket="b", output_prefix="processed/",
        last_run_ts="2026-06-01T00:00:00+00:00", overwrite=True,
    )

    assert [p.dataset_id for p in selected] == ["old_ds"]
