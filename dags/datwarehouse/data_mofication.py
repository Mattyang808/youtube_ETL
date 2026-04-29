import logging
logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'video_id'
            query = f"""
            INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Like_Count", "Comment_Count")
            VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
            """
        else:
            video_id = 'Video_Id'
            query = f"""
            INSERT INTO {schema}.{table} ("Video_Id", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Like_Count", "Comment_Count")
            VALUES (%(Video_Id)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Views)s, %(Like_Count)s, %(Comment_Count)s);
            """
            
        cur.execute(query, row)
        conn.commit()
        logger.info(f"Inserted row with video_id: {row[video_id]} into {schema}.{table}")

    except Exception as e:
        logger.error(f"Error inserting row with video_id: {row[video_id]} into {schema}.{table} - {e}")
        raise e
    
def update_rows(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            duration = 'duration'
            video_views = 'viewCount'
            like_count = 'likeCount'
            comment_count = 'commentCount'
        else:
            video_id = 'Video_Id'
            upload_date = 'Upload_Date'
            video_title = 'Video_Title'
            duration = 'Duration'
            video_views = 'Video_Views'
            like_count = 'Like_Count'
            comment_count = 'Comment_Count' 

        query = f"""
        UPDATE {schema}.{table}
        SET "Video_Title" = %({video_title})s,
            "Upload_Date" = %({upload_date})s,
            "Duration" = %({duration})s,
            "Video_Views" = %({video_views})s,
            "Like_Count" = %({like_count})s,
            "Comment_Count" = %({comment_count})s
        WHERE "Video_Id" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
        """
        
        cur.execute(query, row)
        conn.commit()
        logger.info(f"Updated row with video_id: {row[video_id]} in {schema}.{table}")

    except Exception as e:
        logger.error(f"Error updating row with video_id: {row[video_id]} in {schema}.{table} - {e}")
        raise e
    
def delete_rows(cur, conn, schema, id_to_delete):
    try:
        # Note: Be careful with SQL injection here, usually passing a tuple as params is safer
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in id_to_delete)})"""

        cur.execute(f"""
        DELETE FROM {schema}.{table}
        WHERE "Video_Id" IN {ids_to_delete};
        """)
        conn.commit()
        logger.info(f"Deleted rows with video_ids: {id_to_delete} from {schema}.{table}")
    except Exception as e:
        logger.error(f"Error deleting rows with video_ids: {id_to_delete} from {schema}.{table} - {e}")
        raise e