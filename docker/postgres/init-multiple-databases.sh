#!/bin/bash
set -e
set -u

function create_user_and_database() {
    local username="$1"
    local password="$2"
    local database="$3"
    echo "Creating user '$username' and database '$database'..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $username WITH PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;

EOSQL
    echo "User '$username' and database '$database' created successfully."
}

# Create Airflow metadata database and user (Fixed variable name and order)
create_user_and_database "$METADATA_DATABASE_USERNAME" "$METADATA_DATABASE_PASSWORD" "$METADATA_DATABASE_NAME"

# Create Celery backend database and user (Fixed order)
create_user_and_database "$CELERY_BACKEND_USERNAME" "$CELERY_BACKEND_PASSWORD" "$CELERY_BACKEND_NAME"

# ETL database and user (Fixed order)
create_user_and_database "$ETL_DATABASE_USERNAME" "$ETL_DATABASE_PASSWORD" "$ETL_DATABASE_NAME"

echo "All databases and users created successfully."