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

# Create Airflow metadata database and user
create_user_and_database $METADATA_DATABASE_USER $METADATA_DATABASE_USERNAME $METADATA_DATABASE_PASSWORD

# Create Celery backend database and user
create_user_and_database $CELERY_BACKEND_NAME $CELERY_BACKEND_USERNAME $CELERY_BACKEND_PASSWORD

#ETL database and user
create_user_and_database $ETL_DATABASE_NAME $ETL_DATABASE_USERNAME $ETL_DATABASE_PASSWORD

echo "All databases and users created successfully."