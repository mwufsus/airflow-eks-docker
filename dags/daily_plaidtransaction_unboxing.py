import datetime

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

date_string = datetime.now().date() - timedelta(1)

default_arg = {
    "owner": "Matt W",
    "start_date": datetime(2020, 11, 17),
    "backfill": False,
    "catchup": False,
}

dag = DAG(
    "daily_plaidtransaction_unboxing",
    default_args=default_arg,
    schedule_interval="15 6 * * *",
    template_searchpath="/opt/airflow/data-warehouse/jobs/daily_staging_cleanup/",
)

staging_plaidtransaction_unbox_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="staging_plaidtransaction_unbox_task",
    sql="staging_unbox_plaidtransaction.sql",
)

staging_creditgenie_plaidtransaction_cg_unbox_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="staging_creditgenie_plaidtransaction_cg_unbox_task",
    sql="staging_creditgenie_unbox_plaidtransaction.sql",
)


staging_plaidtransaction_unbox_task
staging_creditgenie_plaidtransaction_cg_unbox_task
