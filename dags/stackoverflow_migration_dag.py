from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import anthropic
import clickhouse_connect
from datetime import datetime, timedelta

# Project path for your dbt models
DBT_PROJECT_DIR = "/opt/airflow/stackoverflow_analytics"

def run_llm_infra_setup():
    """
    Retrieves credentials, asks Claude Opus 4.7 to generate SQL, 
    and executes it in the existing 'big_query' database.
    """
    # 1. Fetch Variables
    api_key = Variable.get("ANTHROPIC_API_KEY")
    ch_host = Variable.get("CH_HOST")
    ch_user = Variable.get("CH_USER")
    ch_pass = Variable.get("CH_PASSWORD")

    # 2. Initialize Anthropic Client
    client = anthropic.Anthropic(api_key=api_key)
    
    system_prompt = (
        "You are a ClickHouse Expert. Respond ONLY with valid SQL statements. "
        "Do not include markdown code blocks, explanations, or introductory text. "
        "Ensure you use 'CREATE TABLE IF NOT EXISTS' and 'CREATE MATERIALIZED VIEW IF NOT EXISTS' for idempotency."
    )
    
    user_prompt = """
    Infer Schema: Run DESCRIBE TABLE s3(
    'https://bigquerybuckets3.s3.us-east-1.amazonaws.com/raw-data/stackoverflow/*.parquet',
    'Parquet',
    extra_credentials(role_arn = 'arn:aws:iam::227855914163:role/ClickHouse-S3-ReadOnly-StackOverflow'));

    Create Target: Create the table big_query.stackoverflow_final using MergeTree. Automate the types: Map the Parquet types to ClickHouse types (e.g., DateTime64 for dates). Use id and creation_date for the ORDER BY.

    Create Queue: Create big_query.stackoverflow_queue using the S3Queue engine with these same inline credentials.

    Connect: Create the Materialized View big_query.stackoverflow_mv to start the flow.

    Verify: Tell me once the first few rows appear in big_query.stackoverflow_final."
    """

    # 3. Fetch generated SQL using Adaptive Thinking (Claude Opus 4.7)
    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=4000, 
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
        thinking={
            "type": "adaptive"
        }
    )
    
    # Extract text content (ignoring thinking blocks)
    sql_commands = ""
    for block in response.content:
        if block.type == "text":
            sql_commands = block.text
            break
    
    # 4. Establish connection to ClickHouse
    ch_client = clickhouse_connect.get_client(
        host=ch_host, 
        username=ch_user, 
        password=ch_pass, 
        port=8443, 
        secure=True
    )
    
    # Verify the 'big_query' database is accessible
    ch_client.command("USE big_query")
    print("--- GENERATED SQL FROM CLAUDE ---")
    print(sql_commands)
    
    # 5. Execute SQL with individual error catching
    statements = [s.strip() for s in sql_commands.split(';') if s.strip()]
    for statement in statements:
        try:
            ch_client.command(statement)
            print(f"Successfully Executed: {statement[:60]}...")
        except Exception as e:
            print(f"FAILED to execute statement: {statement[:60]}...")
            print(f"Error detail: {e}")
            raise

# --- DAG Definition ---

default_args = {
    'owner': 'Aniket Shinde',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 4,                                
    'retry_delay': timedelta(seconds=10),      
}

with DAG(
    'stackoverflow_medallion_pipeline_v2',
    default_args=default_args,
    description='Automated Migration: Glue -> LLM Infra Setup -> dbt',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    trigger_glue_migration = GlueJobOperator(
        task_id='trigger_glue_migration',
        job_name='BigQ_to_S3_2',
        region_name='us-east-1',
    )

    automate_clickhouse_setup = PythonOperator(
        task_id='AUTOMATE_CLICKHOUSE_SETUP',
        python_callable=run_llm_infra_setup,
    )

    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select staging',
    )

    run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select staging',
    )

    run_dbt_marts = BashOperator(
        task_id='run_dbt_marts',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --select marts',
    )

    trigger_glue_migration >> automate_clickhouse_setup >> run_dbt_staging >> run_dbt_tests >> run_dbt_marts