import os.path

import pendulum
from airflow.models import DagBag

DAG_BAG = os.path.join(os.path.dirname(__file__), "../dags")


class TestDagConfig:
    dagbag = DagBag(dag_folder=DAG_BAG, include_examples=False)

    def test_dag_config(self):
        assert self.dagbag.import_errors == {}, self.dagbag.import_errors
        dag = self.dagbag.get_dag(dag_id="breaking_bad_quotes")
        assert dag.schedule_interval in ["*/5 * * * *", "0/5 * * * *"]
        assert dag.catchup is False
        assert dag.description == "A simple DAG to store breaking bad quotes"
        assert dag.start_date == pendulum.today("UTC").add(days=-1)
        assert dag.default_args == {"depends_on_past": False}
