import pandas as pd

from chem_pipeline.data_quality import run_data_quality_checks

CFG = {
    "min_valid_smiles_ratio": 0.90,
    "max_duplicate_ratio": 0.20,
    "max_missing_property_ratio": 0.02,
    "molwt_range": [50, 1500],
    "logp_range": [-6, 10],
}


def _good_df():
    return pd.DataFrame({
        "smiles": ["CCOc1ccccc1", "CCNc1ccccc1", "CCCc1ccccc1"],
        "canonical_smiles": ["CCOc1ccccc1", "CCNc1ccccc1", "CCCc1ccccc1"],
        "mol_wt": [122.16, 121.18, 120.19],
        "log_p": [1.9, 1.6, 2.9],
    })


def test_passes_on_clean_data():
    result = run_data_quality_checks(_good_df(), "ds1", CFG)
    assert result.passed
    assert result.failures == []


def test_fails_on_empty_dataframe():
    empty = pd.DataFrame(columns=["smiles", "canonical_smiles", "mol_wt", "log_p"])
    result = run_data_quality_checks(empty, "ds_empty", CFG)
    assert not result.passed
    assert "empty dataset" in result.failures[0].lower()


def test_fails_on_invalid_smiles_ratio():
    df = pd.DataFrame({
        "smiles": ["CCO", "not_a_smiles", "also_bad", "still_bad"],
        "canonical_smiles": ["CCO", "not_a_smiles", "also_bad", "still_bad"],
        "mol_wt": [46.07, None, None, None],
        "log_p": [-0.14, None, None, None],
    })
    result = run_data_quality_checks(df, "ds_bad", CFG)
    assert not result.passed
    assert any("valid_smiles_ratio" in f for f in result.failures)


def test_fails_on_high_duplicate_ratio():
    df = pd.DataFrame({
        "smiles": ["CCO"] * 4 + ["CCN"],
        "canonical_smiles": ["CCO"] * 4 + ["CCN"],
        "mol_wt": [46.07] * 5,
        "log_p": [-0.14] * 5,
    })
    result = run_data_quality_checks(df, "ds_dup", CFG)
    assert not result.passed
    assert any("duplicate_ratio" in f for f in result.failures)
