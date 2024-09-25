import pytest
from tests import lewagonde
from yaml import load
from yaml.loader import SafeLoader


@pytest.mark.optional
def test_docker_compose():
    with open("docker-compose.yml") as f:
        docker_compose_data = load(f, SafeLoader)

    assert docker_compose_data.get("version") == "3"
    assert set(docker_compose_data.get("services", {}).keys()) == {
        "postgres",
        "scheduler",
        "webserver",
    }
    postgres = docker_compose_data["services"]["postgres"]
    assert postgres.get("image").startswith("postgres:14") is True
    assert lewagonde.dict_or_kvlist_to_dict(postgres.get("environment")) == {
        "POSTGRES_DB": "db",
        "POSTGRES_PASSWORD": "$POSTGRES_PASSWORD",
        "POSTGRES_USER": "airflow",
    }
    assert postgres.get("volumes") == ["./database/:/var/lib/postgresql/data"]
    assert postgres.get("healthcheck", {}).get("test") in [
        [
            "CMD",
            "pg_isready -d db -U airflow",
        ],
        ["CMD", "pg_isready", "-d", "db", "-U", "airflow"],
    ]
    assert postgres.get("healthcheck", {}).get("interval") == "5s"
    assert postgres.get("healthcheck", {}).get("retries") == 5
    assert postgres.get("ports") == ["5433:5432"]
    assert postgres.get("restart") == "always"

    scheduler = docker_compose_data["services"]["scheduler"]
    assert scheduler.get("build") == "."
    assert scheduler.get("command") == "poetry run airflow scheduler"
    assert scheduler.get("restart") == "on-failure"
    assert scheduler.get("depends_on") == ["postgres"]
    assert lewagonde.dict_or_kvlist_to_dict(scheduler.get("environment")) == {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://airflow:$POSTGRES_PASSWORD@postgres:5432/db",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
    }
    assert set(scheduler.get("volumes")) == {
        "./dags:/app/airflow/dags",
        "./data:/app/airflow/data",
        "./logs:/app/airflow/logs",
    }

    webserver = docker_compose_data["services"]["webserver"]
    assert webserver.get("build") == "."
    assert webserver.get("command") in [
        "poetry run scripts/entrypoint.sh",
        "poetry run ./scripts/entrypoint.sh",
    ]
    assert webserver.get("restart") == "on-failure"
    assert set(webserver.get("depends_on")) == {"postgres", "scheduler"}
    assert lewagonde.dict_or_kvlist_to_dict(webserver.get("environment")) == {
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://airflow:$POSTGRES_PASSWORD@postgres:5432/db",
        "POSTGRES_DB": "db",
        "POSTGRES_PASSWORD": "$POSTGRES_PASSWORD",
        "POSTGRES_USER": "airflow",
    }
    assert set(webserver.get("volumes")) == {
        "./dags:/app/airflow/dags",
        "./data:/app/airflow/data",
        "./logs:/app/airflow/logs",
    }
    assert webserver.get("ports") == ["8080:8080"]
    assert webserver.get("healthcheck", {}).get("test") == [
        "CMD",
        "-f",
        "/home/airflow/airflow-webserver.pid",
    ]
    assert webserver.get("healthcheck", {}).get("interval") == "30s"
    assert webserver.get("healthcheck", {}).get("timeout") == "30s"
    assert webserver.get("healthcheck", {}).get("retries") == 3
