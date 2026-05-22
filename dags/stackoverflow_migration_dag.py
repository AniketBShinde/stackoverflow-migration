from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import anthropic
import clickhouse_connect
import time
from datetime import datetime, timedelta

# Project path for your dbt models
DBT_PROJECT_DIR = "/opt/airflow/stackoverflow_analytics"

# ---------------------------------------------------------------------------
# Wait-for-ingest tuning
# ---------------------------------------------------------------------------
# How often to poll the target table's row count (seconds).
POLL_INTERVAL_SECONDS = 60

# Declare ingest "done" only after the row count has been IDENTICAL for this
# many consecutive polls. Higher = safer (less chance of declaring done while
# rows are still trickling in). Lower = faster.
REQUIRED_STABLE_POLLS = 3

# Hard upper bound on total wait. Tune to your largest expected backfill.
MAX_WAIT_HOURS = 3


def _get_clickhouse_client():
    """Helper - single place to build the CH client."""
    return clickhouse_connect.get_client(
        host=Variable.get("CH_HOST"),
        username=Variable.get("CH_USER"),
        password=Variable.get("CH_PASSWORD"),
        port=8443,
        secure=True,
    )


def run_llm_infra_setup():
    """
    Retrieves credentials, asks Claude to generate SQL,
    and executes it in the existing 'big_query' database.
    """
    # 1. Fetch Variables
    api_key = Variable.get("ANTHROPIC_API_KEY")

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

    Create Target: Create the table big_query.stackoverflow_final using MergeTree. Automate the types: Map the Parquet types to ClickHouse types (e.g., DateTime64 for dates). Use tags and creation_date for the ORDER BY.

    Create Queue: Create big_query.stackoverflow_queue using the S3Queue engine with these same inline credentials.

    Connect: Create the Materialized View big_query.stackoverflow_mv to start the flow.

    Verify: Tell me once the first few rows appear in big_query.stackoverflow_final."
    """

    # 3. Fetch generated SQL using Adaptive Thinking
    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=4000,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
        thinking={"type": "adaptive"},
    )

    # Extract text content (ignoring thinking blocks)
    sql_commands = ""
    for block in response.content:
        if block.type == "text":
            sql_commands = block.text
            break

    # 4. Establish connection to ClickHouse
    ch_client = _get_clickhouse_client()
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


def wait_for_s3queue_completion(**context):
    """
    Block until S3Queue ingest into stackoverflow_final has finished.

    Strategy: poll count(*) every POLL_INTERVAL_SECONDS. Declare done only
    after the count has been IDENTICAL for REQUIRED_STABLE_POLLS consecutive
    polls. This is robust to:
      - async flushing of system.s3queue_log (we don't read it)
      - small pauses between S3Queue picking up files (we wait through them)
      - empty starts (rows must be > 0 before we even consider stability)

    The previous version of this task used a "queue log is quiet" signal,
    which declared done prematurely because s3queue_log entries are flushed
    asynchronously - the last file finished 3 seconds before the sensor's
    "last 3 min" check but the log entry wasn't yet visible to the query.
    Polling the actual target table sidesteps that entirely.
    """
    ch = _get_clickhouse_client()

    prev_count = None
    stable_polls = 0
    elapsed = 0
    max_wait_seconds = MAX_WAIT_HOURS * 3600

    while elapsed < max_wait_seconds:
        current = ch.query(
            "SELECT count() FROM big_query.stackoverflow_final"
        ).result_rows[0][0]

        # Also report queue-log status for visibility (not used in the decision)
        try:
            failed = ch.query(
                "SELECT count() FROM system.s3queue_log "
                "WHERE database = 'big_query' AND status = 'Failed'"
            ).result_rows[0][0]
        except Exception:
            failed = 'n/a'

        if prev_count is not None and current == prev_count and current > 0:
            stable_polls += 1
            print(
                f"[wait_for_ingest] rows={current:,} (unchanged) | "
                f"stable {stable_polls}/{REQUIRED_STABLE_POLLS} polls | "
                f"failed_files={failed} | elapsed={elapsed}s"
            )
            if stable_polls >= REQUIRED_STABLE_POLLS:
                print(
                    f"[wait_for_ingest] Row count stable at {current:,} for "
                    f"{stable_polls} consecutive polls. Ingest complete."
                )
                if failed not in (0, 'n/a'):
                    print(
                        f"[wait_for_ingest] NOTE: {failed} S3 files failed and "
                        "their rows are NOT in stackoverflow_final. Inspect: "
                        "SELECT file_name, exception FROM system.s3queue_log "
                        "WHERE status='Failed';"
                    )
                return
        else:
            stable_polls = 0
            delta = current - (prev_count or 0)
            print(
                f"[wait_for_ingest] rows={current:,} (was {prev_count}, +{delta:,}) | "
                f"stability reset | failed_files={failed} | elapsed={elapsed}s"
            )

        prev_count = current
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    raise TimeoutError(
        f"Ingest did not stabilize within {MAX_WAIT_HOURS} hours. "
        f"Last observed count: {prev_count:,}. "
        "Inspect S3Queue and system.s3queue_log."
    )


# --- DAG Definition --------------------------------------------------------

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
    description='Automated Migration: Glue -> LLM Infra Setup -> Wait for Ingest -> dbt',
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

    # Block dbt until stackoverflow_final stops growing. The previous sensor
    # relied on the async-flushed system.s3queue_log being "quiet" and declared
    # done after only 16M of ~23M rows had arrived. This version polls the
    # actual target table - immune to log-flush timing.
    #
    # retries=0 on this task because the polling loop is self-healing; a retry
    # would just restart the same wait from scratch and waste time.
    wait_for_ingest = PythonOperator(
        task_id='wait_for_s3queue_completion',
        python_callable=wait_for_s3queue_completion,
        retries=0,
        execution_timeout=timedelta(hours=MAX_WAIT_HOURS + 1),
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

    (
        trigger_glue_migration
        >> automate_clickhouse_setup
        >> wait_for_ingest
        >> run_dbt_staging
        >> run_dbt_tests
        >> run_dbt_marts
    )