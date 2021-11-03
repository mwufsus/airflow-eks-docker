import os
import datetime
import sys
import yaml

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from postgres_operator_helper import (
    create_sensor_task,
    create_alpha_task,
    create_copy_task,
)

SLACK_CONN_ID = "Slack"
date_string = datetime.now().date() - timedelta(1)
config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "config"))


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
    "owner": "Kyle G",
    "start_date": datetime(2021, 8, 2),
    "catchup": False,
    "on_failure_callback": task_fail_slack_alert,
}
dag = DAG(
    "counselorgenie_batch_etl",
    default_args=default_arg,
    schedule_interval="45 04 * * *",
    template_searchpath="/opt/airflow/data-warehouse/jobs/counselorgenie_etl/",
    concurrency=2,
    max_active_runs=1,
)

"""
Tasks for Nightly ETL job.  These are dynamically generated via the
datagenie_etl.yml file within the config folder.  New objects need to be added
to the YAML file with the exact naming convention of the s3 key.  An approproate
.sql file will need to be stashed within ./data-warehoue/jobs/daily_batch_etl/
following the same naming convention as the s3 key. 
E.g. Application.sql -> /prod/procurement/Application/
"""

with open(os.path.join(config_dir, "counselorgenie_etl.yml")) as etl_file:
    config_file = yaml.safe_load(etl_file)

    for model in config_file["models"]:
        if model == 'Referral':
            environment = 'referralgenie'
        else:
            environment = 'counselorgenie'
        
        etl_sensor_task = S3KeySensor(
            dag=dag,
            task_id=model + "_sensor",
            bucket_key=f"{environment}/{model}/{date_string}/*",
            bucket_name="datagenie-data",
            aws_conn_id="s3_connection",
            soft_fail=True,
            poke_interval=60*60*24,
            timeout=60*60*24*3,
            mode="reschedule",
            wildcard_match=True,
        )

        etl_copy_task = PostgresOperator(
            dag=dag,
            postgres_conn_id="redshift_default",
            task_id=model + "_copy",
            sql=model + ".sql",
            params={"date_string": date_string},
        )

        vacuum_task = PostgresOperator(
            dag=dag,
            postgres_conn_id="redshift_default",
            task_id="vacuum_task",
            trigger_rule="all_done",
            sql="COMMIT;VACUUM FULL;COMMIT;",
        )

        analyze_task = PostgresOperator(
            dag=dag,
            postgres_conn_id="redshift_default",
            task_id="analyze_task",
            trigger_rule="all_done",
            sql="ANALYZE;",
        )

        etl_sensor_task >> etl_copy_task >> vacuum_task >> analyze_task
