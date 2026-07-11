"""Optional Step 5: Faerun chemical-space graph.

Builds a TMAP layout over Morgan fingerprints and renders an interactive
Faerun HTML scatter, coloured by KMeans cluster. Gated the same way as the
ChemProp stage: config flag + import availability.
"""
from __future__ import annotations

import logging

import pandas as pd
from rdkit import Chem
from rdkit.Chem import AllChem

logger = logging.getLogger(__name__)


def is_available() -> bool:
    try:
        import faerun  # noqa: F401
        import tmap  # noqa: F401
        return True
    except ImportError:
        return False


def build_graph(df: pd.DataFrame, dataset_id: str, out_html_path: str,
                 smiles_col: str = "smiles", cluster_col: str = "cluster") -> str | None:
    if not is_available():
        logger.warning("faerun/tmap not installed - skipping visualization stage")
        return None

    import tmap as tm
    from faerun import Faerun

    enc = tm.Minhash(2048)
    lf = tm.LSHForest(2048, 128)

    fingerprints = []
    for smiles in df[smiles_col]:
        mol = Chem.MolFromSmiles(smiles)
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048) if mol else None
        fingerprints.append(tm.VectorUchar(list(fp)) if fp else tm.VectorUchar([0] * 2048))

    lf.batch_add(enc.batch_from_binary_array(fingerprints))
    lf.index()
    x, y, s, t, _ = tm.layout_from_lsh_forest(lf)

    faerun_obj = Faerun(view="front", coords=False)
    faerun_obj.add_scatter(
        dataset_id,
        {"x": list(x), "y": list(y), "c": df[cluster_col].tolist(),
         "labels": df[smiles_col].tolist()},
        colormap="tab20",
        point_scale=5,
        has_legend=True,
    )
    faerun_obj.add_tree(f"{dataset_id}_tree", {"from": list(s), "to": list(t)}, point_helper=dataset_id)
    faerun_obj.plot(out_html_path, template="smiles")

    return out_html_path
