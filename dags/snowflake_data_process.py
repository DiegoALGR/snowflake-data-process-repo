from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.dummy_operator import DummyOperator

SLACK_CONN_ID = 'slack_conn'
slack_msg = "All tasks finished correctly :dash:"
slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 11, 30),
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'provide_context': True
}

with DAG('snowflake_data_process', 
        default_args=default_args,
        schedule_interval='*/15 * * * *',
        template_searchpath=['/home/diegogr/airflow/snowflake_data_process_files'],
        catchup=False) as dag:

        t1 = DummyOperator(
            task_id='start'
        )

        t2 = SnowflakeOperator(
            task_id='drop-members-by-group-table',
            snowflake_conn_id='snowflake_conn',
            sql='drop-current-members-by-groups-table.sql'
        )

        t3 = SnowflakeOperator(
            task_id='process-members-by-groups',
            snowflake_conn_id='snowflake_conn',
            sql='process-currents-members-by-groups.sql'
        )

        t4 = SnowflakeOperator(
            task_id='drop-groups-count-by-city-table',
            snowflake_conn_id='snowflake_conn',
            sql='drop-groups-count-by-city-table.sql'
        )

        t5 = SnowflakeOperator(
            task_id='process-currents-groups-by-cities',
            snowflake_conn_id='snowflake_conn',
            sql='process-currents-groups-by-cities.sql'
        )

        t6 = SnowflakeOperator(
            task_id='drop-not-horror-events-table',
            snowflake_conn_id='snowflake_conn',
            sql='drop-not-horror-events-table.sql'
        )

        t7 = SnowflakeOperator(
            task_id='process-not-horror-events-opened',
            snowflake_conn_id='snowflake_conn',
            sql='process-not-horror-events-opened.sql'
        )

        t8 = SnowflakeOperator(
            task_id='drop-groups-averages-table',
            snowflake_conn_id='snowflake_conn',
            sql='drop-groups-averages-table.sql'
        )

        t9 = SnowflakeOperator(
            task_id='process-groups-averages-information',
            snowflake_conn_id='snowflake_conn',
            sql='process-groups-averages-information.sql'
        )

        slack_message = SlackWebhookOperator(
            task_id='send-message-to-slack',
            http_conn_id=SLACK_CONN_ID,
            webhook_token=slack_webhook_token,
            message=slack_msg,
            username='airflow',
            icon_emoji='frog'
        )

        t1 >> t2 >> t3 >> slack_message
        t1 >> t4 >> t5 >> slack_message
        t1 >> t6 >> t7 >> slack_message
        t1 >> t8 >> t9 >> slack_message