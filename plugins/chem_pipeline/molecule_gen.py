"""Molecule generation: combine a scaffold with its R-groups.

Expected input format (as described in the task):
  scaffolds.csv:  columns ["id", "smiles"]   e.g. "CCC*" or "c1ccccc1[*:1]"
  r_groups.csv:   columns ["id", "attachment_point", "smiles"]
                  e.g. id=1, attachment_point=1, smiles="[*:1]CC(=O)O"

Attachment points are represented as RDKit dummy atoms. Where a scaffold has
a single unlabelled "*" it is treated as attachment point 1. Assembly uses
RDKit's `Chem.molzip`, which is purpose-built for stitching fragments back
together at matching dummy-atom labels.
"""
from __future__ import annotations

import itertools
import logging
from typing import Iterable, Iterator

import pandas as pd
from rdkit import Chem

logger = logging.getLogger(__name__)


def _label_default_attachment(smiles: str) -> str:
    """Turn a bare '*' into an isotope-labelled [*:1] dummy so molzip has a
    label to match against, if the SMILES doesn't already use labelled dummies."""
    if "[*:" in smiles:
        return smiles
    return smiles.replace("*", "[*:1]", 1)


def _mol_from_labelled_smiles(smiles: str) -> Chem.Mol:
    mol = Chem.MolFromSmiles(_label_default_attachment(smiles))
    if mol is None:
        raise ValueError(f"RDKit failed to parse SMILES: {smiles!r}")
    return mol


def enumerate_molecules_for_id(scaffold_smiles: str, r_group_smiles_list: Iterable[str]) -> Iterator[Chem.Mol]:
    """Yield every scaffold+R-group combination as an assembled RDKit Mol.

    If a scaffold has multiple attachment points ([*:1], [*:2], ...) and
    r_group_smiles_list contains candidates for each, this yields the
    combinatorial product across attachment points grouped by label.
    """
    scaffold_mol = _mol_from_labelled_smiles(scaffold_smiles)

    # Group R-groups by which attachment-point label they target.
    by_label: dict[str, list[str]] = {}
    for rg_smiles in r_group_smiles_list:
        labelled = _label_default_attachment(rg_smiles)
        rg_mol = Chem.MolFromSmiles(labelled)
        if rg_mol is None:
            logger.warning("Skipping unparseable R-group SMILES: %s", rg_smiles)
            continue
        labels = {a.GetIsotope() for a in rg_mol.GetAtoms() if a.GetAtomicNum() == 0}
        label = next(iter(labels), 1)
        by_label.setdefault(str(label), []).append(labelled)

    if not by_label:
        return

    labels_sorted = sorted(by_label.keys())
    for combo in itertools.product(*(by_label[l] for l in labels_sorted)):
        try:
            assembled = scaffold_mol
            for rg_smiles in combo:
                rg_mol = Chem.MolFromSmiles(rg_smiles)
                assembled = Chem.molzip(assembled, rg_mol)
            Chem.SanitizeMol(assembled)
            yield assembled
        except Exception as exc:  # noqa: BLE001 - log and skip malformed combos
            logger.warning("molzip failed for scaffold=%s r_groups=%s: %s",
                            scaffold_smiles, combo, exc)


def generate_molecules(scaffold_df: pd.DataFrame, r_groups_df: pd.DataFrame, dataset_id: str) -> pd.DataFrame:
    """Build the full enumerated-molecule table for one dataset id.

    Returns a DataFrame with columns: dataset_id, scaffold_smiles, smiles (generated),
    canonical_smiles.
    """
    rows = []
    for _, srow in scaffold_df.iterrows():
        scaffold_smiles = srow["smiles"]
        r_group_smiles = r_groups_df["smiles"].tolist()
        for mol in enumerate_molecules_for_id(scaffold_smiles, r_group_smiles):
            rows.append({
                "dataset_id": dataset_id,
                "scaffold_smiles": scaffold_smiles,
                "smiles": Chem.MolToSmiles(mol),
                "canonical_smiles": Chem.MolToSmiles(mol, canonical=True),
            })

    result = pd.DataFrame(rows, columns=["dataset_id", "scaffold_smiles", "smiles", "canonical_smiles"])
    logger.info("Generated %d molecules for dataset %s", len(result), dataset_id)
    return result
