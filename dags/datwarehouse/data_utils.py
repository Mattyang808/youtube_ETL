from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = 'yt_api'

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id='AIRFLOW_CONN_POSTGRES__DB_YT_ETL', database='etl_db')

    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_Id" TEXT PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INTEGER,
            "Like_Count" INTEGER,
            "Comment_Count" INTEGER
        )
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_Id" TEXT PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INTEGER,
            "Like_Count" INTEGER,
            "Comment_Count" INTEGER
        )
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Video_Id" FROM {schema}.{table}""")
    ids = cur.fetchall()

    videos_ids = [id['Video_Id'] for id in ids] 

    return videos_ids