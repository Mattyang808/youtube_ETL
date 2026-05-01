from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dataquality.soda import yt_etl_data_quality_check
from datwarehouse.dwh import core_table, staging_table
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_channel_id, get_video_ids, get_video_details, save_to_json

local_tz = pendulum.timezone("Australia/Perth")

#default_args 
default_args = {
    'owner': 'dataengineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'emails': 'ocagent879@gmail.com',
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz),
    #'end_date': datetime(2030, 5, 1, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to extract YouTube video stats and save to JSON',
    schedule='0 14 * * *',  # Run daily at 14:00 (2 PM) Perth time
    catchup=False,
) as dag_produce:
    
    #define tasks
    playlist_id = get_channel_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = get_video_details(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id='trigger_update_db',
        trigger_dag_id='update_db',
    )

    #define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db  


with DAG(

    dag_id='update_db',

    default_args=default_args,

    description='A DAG to extract YouTube video data and update the database',
    schedule = None, # This DAG will be triggered by the produce_json DAG after saving the JSON file
    catchup=False,

    ) as dag_update:

    #define tasks

    update_staging = staging_table()

    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id='trigger_data_quality',
        trigger_dag_id='data_quality',
    )

    #define dependencies

    update_staging >> update_core >> trigger_data_quality   

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='check data quality of both layers in the db',
    schedule = None, # This DAG will be triggered by the update_db DAG after updating the database
    catchup=False,
    ) as dag_quality:

    soda_validate_staging = yt_etl_data_quality_check('staging')

    soda_validate_core = yt_etl_data_quality_check('core')

    soda_validate_staging >> soda_validate_core