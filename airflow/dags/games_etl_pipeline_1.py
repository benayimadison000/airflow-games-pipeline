from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import io
import logging

# ── Config ──────────────────────────────────────────────────────────────────
DB_CONN           = "postgresql+psycopg2://postgres:madison@172.28.128.1:5432/airflow"
API_URL           = "https://www.freetogame.com/api/games"
MIN_EXPECTED_ROWS = 100

log = logging.getLogger(__name__)

default_args = {
    'owner': 'ben',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'on_failure_callback': None,
}

# ── Extract ──────────────────────────────────────────────────────────────────
def extract(**context):
    log.info(f"Fetching data from {API_URL}")
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    games = response.json()
    log.info(f"Extracted {len(games)} games from API")
    context['ti'].xcom_push(key='raw_games', value=games)

# ── Transform ────────────────────────────────────────────────────────────────
def transform(**context):
    games = context['ti'].xcom_pull(key='raw_games', task_ids='extract')
    log.info(f"Transforming {len(games)} games")
    df = pd.DataFrame(games)
    df = df[[
        'id', 'title', 'genre', 'platform',
        'publisher', 'developer', 'release_date', 'short_description'
    ]].rename(columns={
        'id':                'game_id',
        'short_description': 'description',
    })
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['title']        = df['title'].str.strip()
    df['genre']        = df['genre'].str.strip().str.lower()
    df['platform']     = df['platform'].str.strip().str.lower()
    df['publisher']    = df['publisher'].str.strip()
    df['developer']    = df['developer'].str.strip()
    df['ingested_at']  = datetime.now()
    df = df.dropna(subset=['game_id', 'title'])
    log.info(f"Transformed {len(df)} games — {df['genre'].nunique()} genres found")
    context['ti'].xcom_push(
        key='transformed_games',
        value=df.to_json(orient='records', date_format='iso')
    )
    context['ti'].xcom_push(key='row_count', value=len(df))

# ── Load ─────────────────────────────────────────────────────────────────────
def load(**context):
    data_json = context['ti'].xcom_pull(key='transformed_games', task_ids='transform')
    df = pd.read_json(io.StringIO(data_json), orient='records')
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['ingested_at']  = pd.to_datetime(df['ingested_at'], errors='coerce')

    engine = create_engine(DB_CONN)

    with engine.begin() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS games_raw (
                game_id       INTEGER PRIMARY KEY,
                title         VARCHAR(255),
                genre         VARCHAR(100),
                platform      VARCHAR(100),
                publisher     VARCHAR(255),
                developer     VARCHAR(255),
                release_date  DATE,
                description   TEXT,
                ingested_at   TIMESTAMP
            )
        '''))

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text('''
                INSERT INTO games_raw (
                    game_id, title, genre, platform,
                    publisher, developer, release_date,
                    description, ingested_at
                )
                VALUES (
                    :game_id, :title, :genre, :platform,
                    :publisher, :developer, :release_date,
                    :description, :ingested_at
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    title        = EXCLUDED.title,
                    genre        = EXCLUDED.genre,
                    platform     = EXCLUDED.platform,
                    publisher    = EXCLUDED.publisher,
                    developer    = EXCLUDED.developer,
                    release_date = EXCLUDED.release_date,
                    description  = EXCLUDED.description,
                    ingested_at  = EXCLUDED.ingested_at
            '''), row.to_dict())

    log.info(f"Loaded {len(df)} games into games_raw")

# ── Validate ─────────────────────────────────────────────────────────────────
def validate(**context):
    engine = create_engine(DB_CONN)

    with engine.connect() as conn:
        result   = conn.execute(text("SELECT COUNT(*) FROM games_raw"))
        db_count = result.scalar()

    transform_count = context['ti'].xcom_pull(key='row_count', task_ids='transform')

    log.info(f"Rows in DB:        {db_count}")
    log.info(f"Rows transformed:  {transform_count}")
    log.info(f"Minimum expected:  {MIN_EXPECTED_ROWS}")

    if db_count < MIN_EXPECTED_ROWS:
        raise ValueError(
            f"Validation FAILED: expected at least {MIN_EXPECTED_ROWS} rows "
            f"in games_raw but found {db_count}. Pipeline halted."
        )

    if transform_count and db_count < (transform_count * 0.9):
        raise ValueError(
            f"Validation FAILED: transformed {transform_count} rows but only "
            f"{db_count} in DB — possible data loss. Pipeline halted."
        )

    log.info("Validation PASSED.")
    context['ti'].xcom_push(key='validated_db_count', value=db_count)

# ── Summarise ─────────────────────────────────────────────────────────────────
def summarise(**context):
    engine   = create_engine(DB_CONN)
    db_count = context['ti'].xcom_pull(key='validated_db_count', task_ids='validate')

    with engine.connect() as conn:
        genre_rows = conn.execute(text('''
            SELECT
                genre,
                COUNT(*) as game_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct
            FROM games_raw
            GROUP BY genre
            ORDER BY game_count DESC
        ''')).fetchall()

    with engine.connect() as conn:
        platform_rows = conn.execute(text('''
            SELECT platform, COUNT(*) as game_count
            FROM games_raw
            GROUP BY platform
            ORDER BY game_count DESC
            LIMIT 5
        ''')).fetchall()

    with engine.connect() as conn:
        latest_ingest = conn.execute(text(
            "SELECT MAX(ingested_at) FROM games_raw"
        )).scalar()

    log.info("=" * 50)
    log.info("PIPELINE RUN SUMMARY")
    log.info("=" * 50)
    log.info(f"Total games in DB:   {db_count}")
    log.info(f"Last ingested at:    {latest_ingest}")
    log.info("-" * 50)
    log.info("GENRE BREAKDOWN:")
    for row in genre_rows:
        log.info(f"  {row[0]:<20} {row[1]:>4} games  ({row[2]}%)")
    log.info("-" * 50)
    log.info("TOP 5 PLATFORMS:")
    for row in platform_rows:
        log.info(f"  {row[0]:<30} {row[1]:>4} games")
    log.info("=" * 50)

# ── Alert on failure ──────────────────────────────────────────────────────────
def alert_on_failure(context):
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    run_id  = context['run_id']
    log_url = context['task_instance'].log_url

    message = (
        f"AIRFLOW ALERT — Task Failed\n"
        f"DAG:   {dag_id}\n"
        f"Task:  {task_id}\n"
        f"Run:   {run_id}\n"
        f"Logs:  {log_url}"
    )

    log.error("=" * 50)
    log.error(message)
    log.error("=" * 50)

    # ── Slack (uncomment + add webhook URL to enable) ──
    # import requests as req
    # SLACK_WEBHOOK = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    # req.post(SLACK_WEBHOOK, json={"text": message}, timeout=10)

    # ── Email (uncomment + configure SMTP in airflow.cfg to enable) ──
    # from airflow.utils.email import send_email
    # send_email(
    #     to=["your@email.com"],
    #     subject=f"Airflow failure: {dag_id}.{task_id}",
    #     html_content=f"<pre>{message}</pre>"
    # )

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id='games_etl_pipeline_1',
    default_args={
        **default_args,
        'on_failure_callback': alert_on_failure,
    },
    description='Extract, transform, load, validate and summarise games data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['games', 'etl'],
) as dag:


    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    validate_task = PythonOperator(
        task_id='validate',
        python_callable=validate,
    )

    dbt_task = BashOperator(
        task_id='dbt_build',
        bash_command=(
            'source /home/ben/airflow-pipeline/venv/bin/activate && '
            'cd /home/ben/dbt-duckdb-games && '
            'dbt build --full-refresh'
        ),
        env={
            'DBT_PROFILES_DIR': '/home/ben/.dbt',
            'PATH': '/home/ben/airflow-pipeline/venv/bin:/usr/bin:/bin',
        },
    )

    summarise_task = PythonOperator(
        task_id='summarise',
        python_callable=summarise,
    )

    # Full end-to-end dependency chain
    extract_task >> transform_task >> load_task >> validate_task >> dbt_task >> summarise_task
