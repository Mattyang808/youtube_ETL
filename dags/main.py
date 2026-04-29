from airflow import DAG
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
) as dag:
    
    #define tasks
    playlist_id = get_channel_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = get_video_details(video_ids)
    save_to_json_task = save_to_json(extract_data)

    #define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task