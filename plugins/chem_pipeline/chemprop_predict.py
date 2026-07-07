"""Optional Step 4: ChemProp property prediction.

Gated on `optional_stages.enable_chemprop` in config AND on `chemprop` being
importable, so the base Airflow image doesn't need this heavy dependency
unless the stage is actually turned on (see requirements-optional.txt).
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def is_available() -> bool:
    try:
        import chemprop  # noqa: F401
        return True
    except ImportError:
        return False


def predict(df: pd.DataFrame, model_dir: str, smiles_col: str = "smiles") -> pd.DataFrame:
    """Run ChemProp inference using a pre-trained model checkpoint directory
    (e.g. synced from `model_dir` on S3 beforehand by a separate task/DAG).

    Returns the input df with additional `chemprop_*` prediction columns.
    """
    if not is_available():
        logger.warning("chemprop not installed - skipping prediction stage")
        return df

    from chemprop.train import make_predictions
    from chemprop.args import PredictArgs

    with tempfile.TemporaryDirectory() as tmp:
        in_path = Path(tmp) / "to_predict.csv"
        out_path = Path(tmp) / "predictions.csv"
        df[[smiles_col]].to_csv(in_path, index=False)

        args = PredictArgs().parse_args([
            "--test_path", str(in_path),
            "--checkpoint_dir", model_dir,
            "--preds_path", str(out_path),
        ])
        make_predictions(args=args)

        preds = pd.read_csv(out_path)

    preds = preds.add_prefix("chemprop_").rename(columns={f"chemprop_{smiles_col}": smiles_col})
    return df.merge(preds, on=smiles_col, how="left")
