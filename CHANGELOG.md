# Changelog

## v0.3.0 - Iteration 3 (feature/step3-dq-teams-notifications)
- `data_quality.py`: post-clustering checks (valid-SMILES ratio, duplicate
  ratio, missing-property ratio, MolWt/LogP plausible ranges). Configurable
  thresholds in `config/dag_config.yaml`.
- `notifications.py`: MS Teams incoming-webhook alerts for per-dataset DQ
  failures, whole-DAG failures (`on_failure_callback`), and a weekly run
  summary card. Uses an Airflow HTTP connection (`ms_teams_webhook`).
- A dataset that fails DQ still gets its output written (for scientist
  review) but skips the optional ChemProp/Faerun stages and doesn't block
  other datasets in the same run.
- Wired in the optional Step 4 (`chemprop_predict.py`) and Step 5
  (`faerun_viz.py`) stages, gated by `optional_stages.enable_*` config flags
  and by whether the optional libraries are actually installed.

## v0.2.0 - Iteration 2 (feature/step2-weekly-incremental-overwrite)
- DAG now runs on `@weekly` schedule instead of manual-only.
- `discover_all_pairs` scans the whole input prefix and pairs scaffolds/r_groups
  by shared id; `filter_new_or_overwrite` picks only what's new since the
  `chem_pipeline_last_processed_ts` Airflow Variable watermark.
- New `overwrite` param (default `False`): when `True`, reprocesses and
  overwrites every discovered dataset regardless of the watermark.
- `dataset_id` param becomes optional and now means "just backfill this one id".
- Each dataset id fans out into its own dynamically-mapped task instance, so
  a failure on one dataset doesn't block the others in the same run.

## v0.1.0 - Iteration 1 (feature/step1-manual-dataset-pipeline)
- Initial DAG: manually triggered, takes a required `dataset_id` param.
- Molecule generation (scaffold + R-groups via RDKit `molzip`), property
  calculation, and KMeans clustering on Morgan fingerprints.
- Output written to `s3://<bucket>/processed/<id>_clustered.csv`.
