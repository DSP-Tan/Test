import os.path

from airflow.models import DagBag

DAG_BAG = os.path.join(os.path.dirname(__file__), "../dags")

# Set this variable temporarily back to student config but without affecting airflow test configuration.
os.environ["AIRFLOW_HOME"] = "/app/airflow"


class TestTasksConfigs:
    dagbag = DagBag(dag_folder=DAG_BAG, include_examples=False)

    def test_tasks(self):
        assert self.dagbag.import_errors == {}, self.dagbag.import_errors
        dag = self.dagbag.get_dag(dag_id="breaking_bad_quotes")
        assert list(map(lambda task: task.task_id, dag.tasks)) == [
            "create_file_if_not_exist",
            "get_quote_and_save_if_new",
        ]

    def test_create_file_if_not_exist_task(self):
        assert self.dagbag.import_errors == {}, self.dagbag.import_errors
        dag = self.dagbag.get_dag(dag_id="breaking_bad_quotes")
        task = dag.get_task("create_file_if_not_exist")

        assert task.__class__.__name__ == "PythonOperator"
        assert task.python_callable.__name__ == "create_file_if_not_exist"
        assert task.op_kwargs == {"quotes_file": "/app/airflow/data/quotes.csv"}
        assert list(map(lambda task: task.task_id, task.upstream_list)) == []
        assert list(map(lambda task: task.task_id, task.downstream_list)) == [
            "get_quote_and_save_if_new"
        ]

    def test_get_quote_and_save_if_new_task(self):
        assert self.dagbag.import_errors == {}, self.dagbag.import_errors
        dag = self.dagbag.get_dag(dag_id="breaking_bad_quotes")
        task = dag.get_task("get_quote_and_save_if_new")

        assert task.__class__.__name__ == "PythonOperator"
        assert task.python_callable.__name__ == "get_quote_and_save_if_new"
        assert task.op_kwargs == {"quotes_file": "/app/airflow/data/quotes.csv"}
        assert list(map(lambda task: task.task_id, task.upstream_list)) == [
            "create_file_if_not_exist"
        ]
        assert list(map(lambda task: task.task_id, task.downstream_list)) == []
