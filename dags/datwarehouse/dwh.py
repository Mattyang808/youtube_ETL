# Assuming datwarehouse is the actual folder name based on your file path
from datwarehouse.data_utils import get_conn_cursor, create_table, close_conn_cursor, get_video_ids, create_schema
from datwarehouse.data_mofication import insert_rows, update_rows, delete_rows
from datwarehouse.data_transformation import transform_data
from datwarehouse.data_loading import load_path_to_json

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = 'yt_api'

@task
def staging_table():
    schema = 'staging'
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        YT_data = load_path_to_json(None)

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
                    
        ids_in_json = [row['video_id'] for row in YT_data]

        ids_to_delete = set(table_ids) - set(ids_in_json)

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"Staging table {schema}.{table} updated successfully with {len(YT_data)} records.")
    
    except Exception as e:
        logger.error(f"Error updating staging table {schema}.{table} - {e}")
        raise e

    finally:
        if cur and conn:
            close_conn_cursor(conn, cur)    

@task
def core_table():
    schema = 'core'
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        current_video_ids = set()

        cur.execute(f"""SELECT * FROM staging.{table}""")
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row['Video_Id'])
            
            transformed_row = transform_data(row)
            
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, transformed_row)
            else:
                if transformed_row['Video_Id'] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"Core table {schema}.{table} updated successfully with {len(rows)} records.")

    except Exception as e: 
        logger.error(f"Error updating core table {schema}.{table} - {e}")
        raise e
        
    finally: 
        if cur and conn:
            close_conn_cursor(conn, cur)