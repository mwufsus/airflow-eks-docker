import datetime
from datetime import timedelta, datetime
import os
import yaml

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow_dbt.operators.dbt_operator import DbtRunOperator
from airflow_dbt.operators.dbt_operator import DbtTestOperator

SLACK_CONN_ID = "Slack"
date_string = datetime.now().date() - timedelta(1)
config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "config"))

# These could be set with environment variables if you want to run the DAG outside the Astro container
PROJECT_HOME = "/opt/airflow"
DBT_PROJECT_DIR = os.path.join(PROJECT_HOME, "dbt")
DBT_MODEL_DIR = os.path.join(DBT_PROJECT_DIR, "models")
DBT_CREDITGENIE_DIR = os.path.join(DBT_MODEL_DIR, "creditgenie")
DBT_TARGET = os.environ.get("DBT_TARGET")
DBT_TARGET_DIR = os.path.join(DBT_PROJECT_DIR, "target")
DBT_DOCS_DIR = os.path.join(PROJECT_HOME, "include", "dbt_docs")


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :michael-scott-no: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="Slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)


default_arg = {
    "owner": "Kyle G/Matt W",
    "start_date": datetime(2021, 11, 2),
    "catchup": False,
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    "counselorgenie_dimensions_etl",
    default_args=default_arg,
    schedule_interval="15 05 * * *",
    concurrency=2,
    max_active_runs=1,
    
)

"""
Tasks for Nightly transforms job.  These are dynamically generated via the 
datagenie_etl_final.yml file within the config folder.  New objects need to be added
to the YAML file with the exact naming convention of the dbt model (.sql) which
must reside in /dbt/models/creditgenie/dimensional. 
"""

with open(os.path.join(config_dir, "counselorgenie_dimensions.yml")) as etl_file:
    config_file = yaml.safe_load(etl_file)

    for model in config_file["transforms"]:
        etl_transform_task = DbtRunOperator(
            task_id="dbt_run_" + model,
            dir=DBT_CREDITGENIE_DIR,
            models=model,
            profiles_dir=DBT_PROJECT_DIR,
            target=DBT_TARGET,
            dag=dag,
        )

etl_transform_task
