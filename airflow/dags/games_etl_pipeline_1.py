from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine, text
import json
import io

# ── Config ─────────────────────────────────────────────────────────────────
DB_CONN = "postgresql+psycopg2://postgres:madison@172.28.128.1:5432/airflow"
API_URL = "https://www.freetogame.com/api/games"

default_args = {
    'owner': 'ben',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# ── Extract ─────────────────────────────────────────────────────────────────
def extract(**context):
    print(f"Fetching data from {API_URL}")
    
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    
    games = response.json()
    print(f"Extracted {len(games)} games from API")
    
    # Push data to XCom so the next task can access it
    context['ti'].xcom_push(key='raw_games', value=games)

# ── Transform ───────────────────────────────────────────────────────────────
def transform(**context):
    # Pull raw data from XCom
    games = context['ti'].xcom_pull(key='raw_games', task_ids='extract')
    print(f"Transforming {len(games)} games")
    
    df = pd.DataFrame(games)
    
    # Select and rename columns
    df = df[[
        'id', 'title', 'genre', 'platform',
        'publisher', 'developer', 'release_date',
        'short_description'
    ]].rename(columns={
        'id':                'game_id',
        'title':             'title',
        'genre':             'genre',
        'platform':          'platform',
        'publisher':         'publisher',
        'developer':         'developer',
        'release_date':      'release_date',
        'short_description': 'description',
    })
    
    # Clean and cast
    df['release_date']  = pd.to_datetime(df['release_date'], errors='coerce')
    df['title']         = df['title'].str.strip()
    df['genre']         = df['genre'].str.strip().str.lower()
    df['platform']      = df['platform'].str.strip().str.lower()
    df['publisher']     = df['publisher'].str.strip()
    df['developer']     = df['developer'].str.strip()
    df['ingested_at']   = datetime.now()

    # Drop nulls on key columns
    df = df.dropna(subset=['game_id', 'title'])
    
    print(f"Transformed {len(df)} games after cleaning")
    print(f"Genres found: {df['genre'].nunique()}")
    print(f"Platforms found: {df['platform'].unique()}")

    # Push transformed data to XCom
    context['ti'].xcom_push(
        key='transformed_games',
        value=df.to_json(orient='records', date_format='iso')
    )

# ── Load ────────────────────────────────────────────────────────────────────
def load(**context):
    import io
    
    # Pull transformed data from XCom
    data_json = context['ti'].xcom_pull(
        key='transformed_games',
        task_ids='transform'
    )

    df = pd.read_json(io.StringIO(data_json), orient='records')
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['ingested_at']  = pd.to_datetime(df['ingested_at'], errors='coerce')
    
    engine = create_engine(DB_CONN)
    
    # Create table if it doesn't exist
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
    
    # Upsert — insert new rows, update existing ones
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
    
    print(f"Loaded {len(df)} games into games_raw table in Postgres")

# ── DAG ─────────────────────────────────────────────────────────────────────
with DAG(
    dag_id='games_etl_pipeline_1',
    default_args=default_args,
    description='Extract games from API, transform, and load into Postgres',
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

    extract_task >> transform_task >> load_task