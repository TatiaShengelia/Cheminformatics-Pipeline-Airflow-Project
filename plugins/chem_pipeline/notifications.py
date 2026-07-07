"""MS Teams notifications via incoming webhook.

Uses an Airflow HTTP Connection (default conn_id "ms_teams_webhook") whose
`host` field holds the full Teams incoming-webhook URL. Kept dependency-light
(plain `requests`) rather than needing a dedicated provider package.
"""
from __future__ import annotations

import logging

import requests
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

_COLOR_OK = "28A745"
_COLOR_WARN = "FFC107"
_COLOR_FAIL = "DC3545"


def _webhook_url(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    # Support either the connection's `host` or a `webhook_url` extra field.
    return conn.host or conn.extra_dejson.get("webhook_url")


def _post_card(conn_id: str, title: str, text: str, color: str, facts: list[tuple[str, str]] | None = None) -> None:
    url = _webhook_url(conn_id)
    if not url:
        logger.warning("No MS Teams webhook URL configured for conn_id=%s; skipping notification", conn_id)
        return

    card = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "title": title,
        "text": text,
    }
    if facts:
        card["sections"] = [{"facts": [{"name": k, "value": str(v)} for k, v in facts]}]

    try:
        resp = requests.post(url, json=card, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        # Never let a notification failure fail the DAG run.
        logger.error("Failed to send MS Teams notification: %s", exc)


def notify_dq_failure(conn_id: str, dataset_id: str, failures: list[str], checks: dict) -> None:
    facts = [("dataset_id", dataset_id)] + [(k, v) for k, v in checks.items()]
    _post_card(
        conn_id,
        title=f"⚠️ Data quality failed: {dataset_id}",
        text="Data quality checks failed for this dataset:\n- " + "\n- ".join(failures),
        color=_COLOR_FAIL,
        facts=facts,
    )


def notify_dag_failure(conn_id: str, dag_id: str, run_id: str, exception: str) -> None:
    _post_card(
        conn_id,
        title=f"🛑 {dag_id} failed",
        text=f"Run `{run_id}` failed:\n\n{exception}",
        color=_COLOR_FAIL,
    )


def notify_run_summary(conn_id: str, dag_id: str, results: list[dict]) -> None:
    n_ok = sum(1 for r in results if r.get("passed"))
    n_total = len(results)
    color = _COLOR_OK if n_ok == n_total else (_COLOR_WARN if n_ok > 0 else _COLOR_FAIL)
    facts = [(r["dataset_id"], "✅ passed" if r.get("passed") else "❌ failed DQ") for r in results]
    _post_card(
        conn_id,
        title=f"{dag_id} run summary: {n_ok}/{n_total} datasets OK",
        text="Weekly chem pipeline run complete.",
        color=color,
        facts=facts,
    )
