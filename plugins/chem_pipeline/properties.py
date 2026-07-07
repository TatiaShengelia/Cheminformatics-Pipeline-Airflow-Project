"""Molecular property calculation via RDKit descriptors."""
from __future__ import annotations

import logging

import pandas as pd
from rdkit import Chem
from rdkit.Chem import Crippen, Descriptors, Lipinski, rdMolDescriptors

logger = logging.getLogger(__name__)

PROPERTY_COLUMNS = [
    "mol_wt", "log_p", "hba", "hbd", "tpsa", "rotatable_bonds", "num_rings", "qed",
]


def compute_properties(smiles: str) -> dict:
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {col: None for col in PROPERTY_COLUMNS}
    try:
        qed = Descriptors.qed(mol)
    except Exception:  # noqa: BLE001 - QED can fail on odd valence/fragments
        qed = None
    return {
        "mol_wt": Descriptors.MolWt(mol),
        "log_p": Crippen.MolLogP(mol),
        "hba": Lipinski.NumHAcceptors(mol),
        "hbd": Lipinski.NumHDonors(mol),
        "tpsa": rdMolDescriptors.CalcTPSA(mol),
        "rotatable_bonds": Lipinski.NumRotatableBonds(mol),
        "num_rings": rdMolDescriptors.CalcNumRings(mol),
        "qed": qed,
    }


def add_properties(df: pd.DataFrame, smiles_col: str = "smiles") -> pd.DataFrame:
    props = df[smiles_col].apply(compute_properties).apply(pd.Series)
    result = pd.concat([df.reset_index(drop=True), props.reset_index(drop=True)], axis=1)
    n_failed = result["mol_wt"].isna().sum()
    if n_failed:
        logger.warning("%d/%d molecules failed property calculation", n_failed, len(result))
    return result
