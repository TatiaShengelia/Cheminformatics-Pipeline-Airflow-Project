"""K-means clustering of molecules using Morgan fingerprints."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from rdkit import Chem
from rdkit.Chem import AllChem
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)


def _fingerprint(smiles: str, radius: int, n_bits: int) -> np.ndarray:
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return np.zeros(n_bits, dtype=np.int8)
    fp = AllChem.GetMorganFingerprintAsBitVect(mol, radius, nBits=n_bits)
    arr = np.zeros((n_bits,), dtype=np.int8)
    Chem.DataStructs.ConvertToNumpyArray(fp, arr)
    return arr


def cluster_molecules(df: pd.DataFrame, smiles_col: str = "smiles",
                       n_clusters: int = 8, radius: int = 2, n_bits: int = 2048,
                       random_state: int = 42) -> pd.DataFrame:
    if df.empty:
        df["cluster"] = pd.Series(dtype=int)
        return df

    fps = np.stack(df[smiles_col].apply(lambda s: _fingerprint(s, radius, n_bits)).to_numpy())

    effective_k = min(n_clusters, len(df))
    if effective_k < n_clusters:
        logger.warning("Requested n_clusters=%d but only %d molecules available; using k=%d",
                        n_clusters, len(df), effective_k)

    km = KMeans(n_clusters=effective_k, random_state=random_state, n_init="auto")
    labels = km.fit_predict(fps)

    result = df.reset_index(drop=True).copy()
    result["cluster"] = labels
    return result
