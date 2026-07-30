"""Validate every DAG file through a real Airflow DagBag.

The unit tests import the functions in src/, never the DAG modules in dags/ —
those are only loaded by the scheduler.  A bad import, a renamed helper or a
typo in a task would therefore pass CI and fail in Airflow.  This module
closes that gap.

Skipped when Airflow isn't installed, so a lightweight environment can still
run the rest of the suite.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Airflow reads these when it is first imported, so they have to be set before
# the import below: a throwaway home keeps the test from touching ~/airflow.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-test-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

pytest.importorskip("airflow", reason="Airflow not installed")

from airflow.models import DagBag  # must come after the env setup above

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"

# The DAGs this project ships, and the tasks each one is expected to contain.
EXPECTED_DAGS = {
    "ingest_realtime": {"fetch_trip_updates", "fetch_vehicle_positions"},
    "ingest_static": {"download_and_version"},
    "check_freshness": {"check_trip_updates", "check_vehicle_positions", "check_static"},
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    """Parse dags/ once for the whole module — a DagBag fill is slow."""
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_expected_dags_are_registered(dagbag: DagBag) -> None:
    assert set(dagbag.dag_ids) == set(EXPECTED_DAGS)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_task_ids_match(dagbag: DagBag, dag_id: str) -> None:
    dag = dagbag.dags[dag_id]
    assert {task.task_id for task in dag.tasks} == EXPECTED_DAGS[dag_id]


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dags_are_documented_and_tagged(dagbag: DagBag, dag_id: str) -> None:
    """Tags drive the Airflow UI filters; doc_md renders on the DAG page."""
    dag = dagbag.dags[dag_id]
    assert dag.tags
    assert dag.doc_md


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dags_do_not_backfill(dagbag: DagBag, dag_id: str) -> None:
    """catchup=True on a minute-level feed would stampede on first unpause."""
    assert dagbag.dags[dag_id].catchup is False
