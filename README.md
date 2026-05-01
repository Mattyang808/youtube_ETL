# YouTube ETL Pipeline

A robust, containerized ETL (Extract, Transform, Load) data pipeline powered by Apache Airflow, PostgreSQL, and Soda. This pipeline extracts daily channel and video statistics using the YouTube Data API, loads them into a local Data Warehouse (Star/Snowflake schema via staging and core tables), and executes automated data quality checks.

## 🏗️ Architecture

1. **Extract**: Fetch video and channel stats from the YouTube Data API and save them as JSON.
2. **Transform & Load**: Parse the JSON outputs and populate two layers in PostgreSQL:
   - **Staging Layer**: Raw or lightly cleaned data integration.
   - **Core Layer**: Final reporting/analytics ready tables.
3. **Data Quality**: Validates anomalies, missing values, and logical rule constraints (e.g., likes cannot exceed views) using [Soda](https://soda.io).

## 🗂️ Project Structure

```
youtube_ETL/
├── config/                  # Airflow configuration files
├── dags/                    # Directed Acyclic Graphs (Airflow Pipelines)
│   ├── api/                 # YouTube API extraction scripts
│   ├── dataquality/         # Logic for Soda data quality checks
│   ├── datwarehouse/        # Logic for staging and core table creation/loading
│   └── main.py              # Main DAGs definition file
├── data/                    # Local storage for extracted JSON files
├── docker/                  # Init scripts for PostgreSQL
├── include/                 # External dependencies (e.g., Soda configurations)
│   └── soda/                # Soda data quality checks (checks.yml) & configs
├── .github/                 # GitHub Actions workflows for CI/CD
│   └── workflows/           # CI/CD pipeline definition (ci_cd_yt_etl.yaml)
├── docker-compose.yaml      # Docker Compose configuration for the Airflow cluster
├── dockerfile               # Custom Docker image instructions
├── requirements.txt         # Python dependencies
└── fernetkeygen.py          # Script for generating Airflow fernet keys
```

## 🔄 Airflow DAGs

The workflow is orchestrated using three separate scheduled DAGs, running sequentially in the Australia/Perth timezone:

| DAG ID | Schedule | Description |
| :--- | :--- | :--- |
| `produce_json` | Daily at 14:00 | Extracts YouTube channel ID, video IDs, and video statistics, then saves the result as daily JSON files (e.g., `Youtube_data_YYYY-MM-DD.json`). |
| `update_db` | Daily at 15:00 | Parses the JSON files and performs an Upsert/Load into the PostgreSQL `staging` table, followed by updating the `core` analytical table. |
| `data_quality` | Daily at 16:00 | Triggers Soda to run automated profiling and data quality checks against both the `staging` and `core` tables. |

## ⚙️ CI/CD Pipeline

The project includes an automated Continuous Integration and Continuous Deployment (CI/CD) pipeline built with **GitHub Actions**.

Whenever code is pushed or a pull request is created on the `main` branch or `feature/*` branches, the pipeline:
1. Detects which files have changed.
2. Automatically builds and pushes a new version of the Airflow Docker image to **Docker Hub** if the `Dockerfile` or `requirements.txt` is updated.
3. Automatically sets up the `docker-compose` environment and runs Pytest unit & integration tests inside the Airflow worker container.
4. Triggers an End-to-End test by running all three Airflow DAGs (`produce_json`, `update_db`, `data_quality`) to ensure pipeline stability.
5. Gracefully tears down the testing infrastructure.

## 🚀 Getting Started

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed & running.
* YouTube Data API v3 Key (Create a project in the [Google Cloud Console](https://console.cloud.google.com/) and enable the YouTube Data API).

### Installation & Setup

**1. Clone the repository**
```bash
git clone https://github.com/Mattyang808/youtube_ETL.git
cd youtube_ETL
```

**2. Configure Environment Variables**
Create a .env file in the root directory. At a minimum, you'll need parameters for your Postgres database, Airflow initialization, and API Keys.
```env
AIRFLOW_VAR_API_KEY=your_youtube_api_key_here
AIRFLOW_VAR_CHANNEL_HANDLE=your_target_channel_handle

ETL_DATABASE_NAME=yt_etl
ETL_DATABASE_USERNAME=etl_user
ETL_DATABASE_PASSWORD=etl_password
POSTGRES_CONN_HOST=postgres
POSTGRES_CONN_PORT=5432
```
*Tip: Use `python fernetkeygen.py` to generate a secure Fernet key for Airflow if required by your .env.*

**3. Start the infrastructure**
Run Docker Compose to build and start Airflow, PostgreSQL, and Redis (Celery backend).
```bash
docker-compose up -d --build
```

**4. Access Airflow UI**
* Navigate to `http://localhost:8080` in your browser.
* Use the default credentials configured in your .env or Compose file (default is often `airflow` / `airflow`).
* Ensure your .env provided Variables/Connections exist in **Airflow -> Admin -> Variables/Connections**.

## 🛡️ Data Quality Checks (Soda)

This project ensures data reliability by validating constraints specifically tailored for YouTube metrics. Checks are located in checks.yml.

Example validations:
* `Video_Id` is never missing and strictly unique.
* Built-in logical test: `Like_Count` cannot be greater than `Video_Views`.
* Built-in logical test: `Comment_Count` cannot be greater than `Video_Views`.

## 🛑 Stopping the Application

To shut down the cluster and clean up running containers:
```bash
docker-compose down
```
*(Append `-v` to the command if you wish to wipe the volumes and start completely fresh).*
You've used 51% of your weekly rate limit. Your weekly rate limit will reset on 4 May at 8:00. [Learn More](https://aka.ms/github-copilot-rate-limit-error)