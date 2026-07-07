"""chem_pipeline DAG - Iteration 3 (current)

Weekly incremental cheminformatics pipeline with data-quality gating and
MS Teams notifications for the scientist team.

Params:
    dataset_id (str, optional): backfill/debug a single dataset id.
    overwrite (bool, default False): reprocess & overwrite everything found,
        ignoring the last-run watermark.

Flow per discovered dataset id (dynamically mapped):
    read -> generate molecules -> properties -> cluster -> DQ check
        -> [if passed] optional ChemProp predict, optional Faerun graph
        -> [if failed] MS Teams alert, stage skipped for that dataset only
Run-level:
    on_failure_callback posts a DAG-failure card to Teams.
    `finalize` posts a run summary card and advances the watermark.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from chem_pipeline.chemprop_predict import predict as chemprop_predict
from chem_pipeline.clustering import cluster_molecules
from chem_pipeline.data_quality import run_data_quality_checks
from chem_pipeline.faerun_viz import build_graph as build_faerun_graph
from chem_pipeline.molecule_gen import generate_molecules
from chem_pipeline.notifications import notify_dag_failure, notify_dq_failure, notify_run_summary
from chem_pipeline.properties import add_properties
from chem_pipeline.s3_utils import (
    DatasetPair,
    discover_all_pairs,
    filter_new_or_overwrite,
    get_pair_for_id,
    read_csv_from_s3,
    write_df_to_s3,
)

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / "config" / "dag_config.yaml"
LAST_RUN_VARIABLE = "chem_pipeline_last_processed_ts"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _on_dag_failure(context) -> None:
    cfg = load_config()
    if not cfg["notifications"]["notify_on_dag_failure"]:
        return
    notify_dag_failure(
        conn_id=cfg["notifications"]["teams_conn_id"],
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        exception=str(context.get("exception")),
    )


@dag(
    dag_id="chem_pipeline",
    schedule="@weekly",
    start_date=pd.Timestamp("2026-01-01"),
    catchup=False,
    max_active_runs=1,
    tags=["cheminformatics", "iteration-3"],
    on_failure_callback=_on_dag_failure,
    params={
        "dataset_id": Param("", type="string",
                             description="Optional: process only this dataset id (backfill/debug). "
                                         "Leave blank for the normal weekly incremental scan."),
        "overwrite": Param(False, type="boolean",
                            description="If true, reprocess and overwrite every discovered dataset, "
                                        "ignoring the last-run watermark."),
    },
)
def chem_pipeline():

    @task
    def discover_datasets(**context) -> list[dict]:
        cfg = load_config()
        dataset_id_param = context["params"]["dataset_id"]
        overwrite = context["params"]["overwrite"]
        hook = S3Hook(aws_conn_id=cfg["aws_conn_id"])
        bucket = cfg["s3"]["bucket"]

        if dataset_id_param:
            pair = get_pair_for_id(
                hook, bucket, cfg["s3"]["input_prefix"], dataset_id_param,
                cfg["s3"]["scaffold_suffix"], cfg["s3"]["r_groups_suffix"],
            )
            return [pair.__dict__]

        all_pairs: dict[str, DatasetPair] = discover_all_pairs(
            hook, bucket, cfg["s3"]["input_prefix"],
            cfg["s3"]["scaffold_suffix"], cfg["s3"]["r_groups_suffix"],
        )
        last_run_ts = Variable.get(LAST_RUN_VARIABLE, default_var=None)
        to_process = filter_new_or_overwrite(
            hook, all_pairs, bucket, cfg["s3"]["output_prefix"], last_run_ts, overwrite,
        )
        logger.info("Discovered %d pairs total, %d selected for processing (overwrite=%s)",
                    len(all_pairs), len(to_process), overwrite)
        return [p.__dict__ for p in to_process]

    @task
    def process_one_dataset(pair_dict: dict, **context) -> dict:
        """generate -> properties -> cluster -> DQ check -> [optional stages].

        Returns a small summary dict (never raises for a DQ failure - that's
        a business-logic outcome, not a task failure, so other mapped
        datasets keep running and we still get a Teams alert for this one).
        """
        cfg = load_config()
        overwrite = context["params"]["overwrite"] or bool(context["params"]["dataset_id"])
        hook = S3Hook(aws_conn_id=cfg["aws_conn_id"])
        bucket = cfg["s3"]["bucket"]
        dataset_id = pair_dict["dataset_id"]

        scaffold_df = read_csv_from_s3(hook, bucket, pair_dict["scaffold_key"])
        r_groups_df = read_csv_from_s3(hook, bucket, pair_dict["r_groups_key"])

        molecules_df = generate_molecules(scaffold_df, r_groups_df, dataset_id)
        if molecules_df.empty:
            raise AirflowFailException(f"No molecules generated for dataset {dataset_id}")

        properties_df = add_properties(molecules_df)
        clustered_df = cluster_molecules(
            properties_df,
            n_clusters=cfg["clustering"]["n_clusters"],
            radius=cfg["clustering"]["fingerprint_radius"],
            n_bits=cfg["clustering"]["fingerprint_n_bits"],
            random_state=cfg["clustering"]["random_state"],
        )

        dq = run_data_quality_checks(clustered_df, dataset_id, cfg["data_quality"])
        if not dq.passed:
            logger.warning("Data quality FAILED for %s: %s", dataset_id, dq.failures)
            if cfg["notifications"]["notify_on_dq_failure"]:
                notify_dq_failure(cfg["notifications"]["teams_conn_id"], dataset_id, dq.failures, dq.checks)
            # Still persist the clustered output for scientist review, but skip
            # the optional heavy stages for a dataset that failed QC.
            out_key = f"{cfg['s3']['output_prefix'].rstrip('/')}/{dataset_id}_clustered.csv"
            write_df_to_s3(hook, clustered_df, bucket, out_key, overwrite=overwrite)
            return {"dataset_id": dataset_id, "n_molecules": len(clustered_df),
                    "output_key": out_key, "passed": False, "dq_failures": dq.failures}

        out_key = f"{cfg['s3']['output_prefix'].rstrip('/')}/{dataset_id}_clustered.csv"
        write_df_to_s3(hook, clustered_df, bucket, out_key, overwrite=overwrite)

        opt_cfg = cfg["optional_stages"]
        if opt_cfg["enable_chemprop"]:
            enriched = chemprop_predict(clustered_df, model_dir=opt_cfg["chemprop_model_dir"])
            chemprop_key = f"{cfg['s3']['output_prefix'].rstrip('/')}/{dataset_id}_chemprop.csv"
            write_df_to_s3(hook, enriched, bucket, chemprop_key, overwrite=overwrite)

        if opt_cfg["enable_faerun"]:
            html_path = f"/tmp/{dataset_id}_faerun.html"
            built = build_faerun_graph(clustered_df, dataset_id, html_path)
            if built:
                faerun_key = f"{cfg['s3']['output_prefix'].rstrip('/')}/{dataset_id}_faerun.html"
                hook.load_file(html_path, key=faerun_key, bucket_name=bucket, replace=overwrite)

        return {"dataset_id": dataset_id, "n_molecules": len(clustered_df),
                "output_key": out_key, "passed": True, "dq_failures": []}

    @task(trigger_rule="all_done")
    def finalize(results: list[dict], **context) -> None:
        cfg = load_config()
        if not context["params"]["dataset_id"]:
            Variable.set(LAST_RUN_VARIABLE, pd.Timestamp.now(tz="UTC").isoformat())

        for r in results:
            status = "OK" if r.get("passed") else f"DQ FAILED ({r.get('dq_failures')})"
            logger.info(" - %s: %d molecules -> %s [%s]",
                        r["dataset_id"], r["n_molecules"], r["output_key"], status)

        if cfg["notifications"]["notify_on_success"] and results:
            notify_run_summary(cfg["notifications"]["teams_conn_id"], "chem_pipeline", results)

    datasets = discover_datasets()
    results = process_one_dataset.expand(pair_dict=datasets)
    finalize(results)


chem_pipeline()
