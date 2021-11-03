import datetime

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

date_string = datetime.now().date()

default_arg = {"owner": "Kyle G", "start_date": datetime(2021, 3, 9), "catchup": False}


dag = DAG(
    "daily_conversions_report",
    default_args=default_arg,
    schedule_interval="59 * * * *",
    template_searchpath="/usr/local/airflow/data-warehouse/dims/daily_page_conversion_report/",
)

temp_apps_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_apps_task",
    sql="temp_apps.sql",
    params={"date_string": date_string},
)

temp_borrowers_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_borrowers_task",
    sql="temp_borrowers.sql",
    params={"date_string": date_string},
)

temp_creditreports_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_creditreports_task",
    sql="temp_creditreports.sql",
    params={"date_string": date_string},
)

temp_debts_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_debts_task",
    sql="temp_debts.sql",
    params={"date_string": date_string},
)

temp_decisions_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_decisions_task",
    sql="temp_decisions.sql",
    params={"date_string": date_string},
)

temp_expenses_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_expenses__task",
    sql="temp_expenses.sql",
    params={"date_string": date_string},
)

temp_plaiditems_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_plaiditems__task",
    sql="temp_plaiditems.sql",
    params={"date_string": date_string},
)

temp_dmpenrollment_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_dmpenrollment_task",
    sql="temp_dmpenrollment.sql",
    params={"date_string": date_string},
)

temp_final_report_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="temp_final_report_task",
    sql="temp_finalreport.sql",
    params={"date_string": date_string},
)

upsert_final_report_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="upsert_final_table_task",
    sql="upsert_final_report.sql",
)

update_final_report_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift_default",
    task_id="update_final_report_task",
    sql="update_final_table.sql",
    params={"date_string": date_string},
)

temp_apps_task >> temp_final_report_task
temp_borrowers_task >> temp_final_report_task
temp_creditreports_task >> temp_final_report_task
temp_debts_task >> temp_final_report_task
temp_decisions_task >> temp_final_report_task
temp_expenses_task >> temp_final_report_task
temp_plaiditems_task >> temp_final_report_task
temp_final_report_task >> upsert_final_report_task
upsert_final_report_task >> update_final_report_task
