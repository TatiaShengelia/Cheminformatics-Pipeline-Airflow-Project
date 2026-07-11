# chem-pipeline-airflow

Airflow DAG that turns weekly `<id>_scaffolds.csv` / `<id>_r_groups.csv` drops in S3
into enumerated molecules, computed properties, KMeans clusters, and (optionally)
ChemProp predictions + a Faerun chemical-space map - with data-quality gating and
MS Teams alerts for the scientist team.

## Pipeline stages

1. **Molecule generation**: combine each scaffold with its matching R-groups
   (attachment points as RDKit dummy atoms, assembled with `Chem.molzip`).
2. **Property calculation**: MolWt, LogP (Crippen), HBA, HBD, TPSA, rotatable
   bonds, ring count.
3. **Clustering**: Morgan fingerprints --> KMeans.
4. *[Optional]* **ChemProp prediction**: only runs if `chemprop` is installed
   and enabled in config.
5. *[Optional]* **Faerun graph**: TMAP layout + Faerun HTML, same opt-in gating.

Data-quality checks run after clustering and gate steps 4/5; failures (and the
final run summary) are posted to an MS Teams channel via incoming webhook.

## Repository layout

```
dags/                     Airflow DAG definitions
plugins/chem_pipeline/    Shared task logic (importable, unit-testable)
config/                   Pipeline configuration (bucket, thresholds, etc.)
tests/                    Unit tests for the plugin modules
```

## Branching policy

This repo follows **feature --> dev --> prod**:

- `prod`: deployed to the production Airflow environment. Only fast-forward
  merges from `dev`, tagged per release.
- `dev`: integration branch. Feature branches merge here first.
- `feature/*`: one branch per unit of work (see CHANGELOG.md for the mapping
  of feature branches to the 3 delivery iterations of this pipeline).

See `CONTRIBUTING.md` for the exact merge sequence.

## Setup

```bash
pip install -r requirements.txt
# optional, only needed for steps 4 & 5:
pip install -r requirements-optional.txt
```

Required Airflow **Connections**:
- `aws_default` (or set `AWS_CONN_ID` in `config/dag_config.yaml`): S3 access.
- `ms_teams_webhook`: HTTP connection whose `host` is the MS Teams incoming
  webhook URL (added in iteration 3).

Required Airflow **Variables** (auto-created on first run if missing):
- `chem_pipeline_last_processed_ts`: ISO timestamp watermark used for
  incremental processing (iteration 2+).

See `config/dag_config.yaml` for bucket name, prefixes, cluster count, and
data-quality thresholds.
