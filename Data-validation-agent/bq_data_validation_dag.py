from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery

# =====================================================================
# 1. SPECIFIC CONFIGURATION
# =====================================================================
PROJECT_ID = "groovy-scarab-502011-g2"

# Source Table Details
SRC_DATASET = "Source_Data"
SRC_TABLE_NAME = "Customer"

# Target Table Details
TGT_DATASET = "Finance"
TGT_TABLE_NAME = "Customer"

# Mismatch / Audit Table Details
AUDIT_DATASET = "Finance"
AUDIT_TABLE_NAME = "validation_mismatches_customer"

# Primary Key Column(s)
PRIMARY_KEYS = ["id"]

# GCS Bucket to Save Results
# REPLACE this with your actual GCS bucket (e.g., the Composer Environment Bucket)
GCS_BUCKET_NAME = "groovy-scarab-502011-g2-validation-logs"  
GCS_OBJECT_NAME = "data-validation/DVT_Results.txt"


# =====================================================================
# 2. HELPER UTILITIES FOR BIGQUERY
# =====================================================================

def build_pk_json_expr(pks, alias):
    pk_elements = ", ".join([f"'{pk}', {alias}.`{pk}`" for pk in pks])
    return f"TO_JSON_STRING(JSON_OBJECT({pk_elements}))"


def build_hash_expr(columns, alias):
    concat_terms = [f"COALESCE(CAST({alias}.`{col}` AS STRING), 'NULL')" for col in columns]
    concat_str = ", '||', ".join(concat_terms)
    return f"TO_HEX(SHA256(CONCAT({concat_str})))"


def build_struct_expr(columns, alias):
    struct_elements = ", ".join([f"{alias}.`{col}`" for col in columns])
    return f"STRUCT({struct_elements})"


def create_audit_table_if_not_exists(client, audit_ref, log_print):
    schema = [
        bigquery.SchemaField("validation_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("primary_key_value", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mismatch_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_row_hash", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("target_row_hash", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("details", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(audit_ref, schema=schema)
    client.create_table(table, exists_ok=True)
    log_print(f"✔️ Audit Mismatch Table verified: {audit_ref.project}.{audit_ref.dataset_id}.{audit_ref.table_id}")


def get_metadata_row_count(client, table_ref, log_print):
    try:
        table = client.get_table(table_ref)
        return table.num_rows
    except Exception as e:
        log_print(f"⚠️ Metadata read failed for {table_ref.table_id}. Falling back to standard query. ({e})")
        query = f"SELECT COUNT(1) as cnt FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`"
        query_job = client.query(query)
        res = list(query_job.result())
        return res[0].cnt if res else 0


def get_duplicate_count(client, table_ref, pks, log_print):
    pk_cols = ", ".join([f"`{pk}`" for pk in pks])
    query = f"""
    SELECT SUM(cnt - 1) as dup_count
    FROM (
      SELECT {pk_cols}, COUNT(1) as cnt
      FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
      GROUP BY {pk_cols}
      HAVING cnt > 1
    )
    """
    try:
        query_job = client.query(query)
        result = list(query_job.result())
        count = result[0].dup_count
        return count if count is not None else 0
    except Exception as e:
        log_print(f"❌ Failed to run duplicate check on {table_ref.table_id}: {e}")
        return -1


def validate_schemas(client, src_ref, tgt_ref):
    query = f"""
    WITH src_schema AS (
      SELECT column_name, data_type
      FROM `{src_ref.project}.{src_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{src_ref.table_id}'
    ),
    tgt_schema AS (
      SELECT column_name, data_type
      FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{tgt_ref.table_id}'
    )
    SELECT 
      COALESCE(s.column_name, t.column_name) AS column_name,
      s.data_type AS source_type,
      t.data_type AS target_type,
      CASE 
        WHEN s.column_name IS NULL THEN 'MISSING IN SOURCE'
        WHEN t.column_name IS NULL THEN 'MISSING IN TARGET'
        WHEN s.data_type != t.data_type THEN 'DATA TYPE MISMATCH'
        ELSE 'MATCH'
      END AS status
    FROM src_schema s
    FULL OUTER JOIN tgt_schema t ON s.column_name = t.column_name
    WHERE s.column_name IS NULL OR t.column_name IS NULL OR s.data_type != t.data_type
    """
    query_job = client.query(query)
    return list(query_job.result())


def get_common_columns(client, src_ref, tgt_ref):
    query = f"""
    SELECT s.column_name 
    FROM `{src_ref.project}.{src_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS` s
    JOIN `{tgt_ref.project}.{tgt_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS` t
      ON s.column_name = t.column_name AND s.data_type = t.data_type
    WHERE s.table_name = '{src_ref.table_id}' AND t.table_name = '{tgt_ref.table_id}'
    """
    query_job = client.query(query)
    return [row.column_name for row in query_job.result()]


def run_row_level_validation(client, src_ref, tgt_ref, audit_ref, common_cols, run_timestamp):
    pk_json_src = build_pk_json_expr(PRIMARY_KEYS, "s")
    pk_json_tgt = build_pk_json_expr(PRIMARY_KEYS, "t")
    hash_expr_src = build_hash_expr(common_cols, "s")
    hash_expr_tgt = build_hash_expr(common_cols, "t")
    struct_src = build_struct_expr(common_cols, "s")
    struct_tgt = build_struct_expr(common_cols, "t")
    
    join_cond = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in PRIMARY_KEYS])
    pk_condition = " OR ".join([f"s.`{pk}` IS NOT NULL" for pk in PRIMARY_KEYS])

    pk_json_out = f"CASE WHEN {pk_condition} THEN {pk_json_src} ELSE {pk_json_tgt} END"

    query = f"""
    INSERT INTO `{audit_ref.project}.{audit_ref.dataset_id}.{audit_ref.table_id}` 
      (validation_time, primary_key_value, mismatch_type, source_row_hash, target_row_hash, details)
    WITH src_data AS (
        SELECT 
            {pk_json_src} AS pk_val,
            {hash_expr_src} AS row_hash,
            {struct_src} AS row_data,
            *
        FROM `{src_ref.project}.{src_ref.dataset_id}.{src_ref.table_id}` s
    ),
    tgt_data AS (
        SELECT 
            {pk_json_tgt} AS pk_val,
            {hash_expr_tgt} AS row_hash,
            {struct_tgt} AS row_data,
            *
        FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.{tgt_ref.table_id}` t
    )
    SELECT
        @run_timestamp AS validation_time,
        {pk_json_out} AS primary_key_value,
        CASE
            WHEN s.pk_val IS NULL THEN 'MISSING_IN_SOURCE'
            WHEN t.pk_val IS NULL THEN 'MISSING_IN_TARGET'
            WHEN s.row_hash != t.row_hash THEN 'HASH_MISMATCH'
        END AS mismatch_type,
        s.row_hash AS source_row_hash,
        t.row_hash AS target_row_hash,
        TO_JSON_STRING(STRUCT(s.row_data AS source_record, t.row_data AS target_record)) AS details
    FROM src_data s
    FULL OUTER JOIN tgt_data t ON {join_cond}
    WHERE s.pk_val IS NULL OR t.pk_val IS NULL OR s.row_hash != t.row_hash
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_timestamp", "TIMESTAMP", run_timestamp)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    return query_job.num_dml_affected_rows


def fetch_and_print_mismatch_details(client, audit_ref, run_timestamp, log_print, limit=10):
    query = f"""
    SELECT 
      validation_time,
      primary_key_value,
      mismatch_type,
      source_row_hash,
      target_row_hash,
      details
    FROM `{audit_ref.project}.{audit_ref.dataset_id}.{audit_ref.table_id}`
    WHERE validation_time = @run_timestamp
    ORDER BY primary_key_value ASC
    LIMIT {limit}
    """
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_timestamp", "TIMESTAMP", run_timestamp)
            ]
        )
        query_job = client.query(query, job_config=job_config)
        rows = list(query_job.result())
        
        if not rows:
            log_print("   (No details to show).")
            return
            
        log_print("\n=========================================================")
        log_print(f"📁 DETAILED ROW-LEVEL MISMATCHES FOR THIS RUN ({len(rows)} Records)")
        log_print("=========================================================")
        for idx, row in enumerate(rows, start=1):
            log_print(f"\n👉 RECORD #{idx}")
            log_print(f"  🔑 Primary Key Value : {row.primary_key_value}")
            log_print(f"  ❌ Mismatch Type     : {row.mismatch_type}")
            log_print(f"  🌐 Source Hash       : {row.source_row_hash}")
            log_print(f"  🌐 Target Hash       : {row.target_row_hash}")
            log_print(f"  📦 Record Payloads (JSON Structs):")
            log_print(f"     {row.details}")
        log_print("=========================================================\n")
    except Exception as e:
        log_print(f"⚠️ Could not pull detailed row metadata: {e}")


# =====================================================================
# 3. CORE VALIDATION ORCHESTRATOR FOR AIRFLOW
# =====================================================================
def run_validation_task(**kwargs):
    client = bigquery.Client(project=PROJECT_ID)
    run_timestamp = datetime.now(timezone.utc)
    
    # Store all logs in an in-memory buffer
    log_buffer = []

    def log_print(message):
        """Standardizes printing to task logs and local in-memory GCS report buffer."""
        print(message)  # Feeds straight to Composer's standard airflow UI log system
        log_buffer.append(message)

    log_print("🚀 Initializing End-to-End BigQuery Data Validation Pipeline...")

    src_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{SRC_DATASET}.{SRC_TABLE_NAME}")
    tgt_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{TGT_DATASET}.{TGT_TABLE_NAME}")
    audit_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE_NAME}")

    # Step 0: Ensure Audit Table exists
    create_audit_table_if_not_exists(client, audit_ref, log_print)

    # Step 1: Count Validation
    log_print("\n📊 Running Row Count checks...")
    src_cnt = get_metadata_row_count(client, src_ref, log_print)
    tgt_cnt = get_metadata_row_count(client, tgt_ref, log_print)
    log_print(f"   Source Row Count: {src_cnt:,}")
    log_print(f"   Target Row Count: {tgt_cnt:,}")
    log_print(f"   Difference      : {src_cnt - tgt_cnt:,}")

    # Step 2: Duplicate Validation
    log_print("\n🔍 Running Duplicate PK checks...")
    src_dups = get_duplicate_count(client, src_ref, PRIMARY_KEYS, log_print)
    tgt_dups = get_duplicate_count(client, tgt_ref, PRIMARY_KEYS, log_print)
    log_print(f"   Source Duplicate PK Rows: {src_dups:,}")
    log_print(f"   Target Duplicate PK Rows: {tgt_dups:,}")

    # Step 3: Column / Schema Validation
    log_print("\n📐 Running Schema / Column Mismatch checks...")
    schema_mismatches = validate_schemas(client, src_ref, tgt_ref)
    if schema_mismatches:
        log_print("   ❌ SCHEMA MISMATCHES DETECTED:")
        for row in schema_mismatches:
            log_print(f"      - Column: '{row.column_name}' | Source Type: {row.source_type} | Target Type: {row.target_type} | Status: {row.status}")
    else:
        log_print("   ✔️ Schemas Match Perfectly.")

    # Step 4: Row-to-Row Hash Validation
    log_print("\n🔗 Correlating Common Comparison Columns...")
    common_cols = get_common_columns(client, src_ref, tgt_ref)
    log_print(f"   Intersecting columns evaluated for hash logic: {len(common_cols)} columns.")

    common_cols_to_hash = [c for c in common_cols if c not in PRIMARY_KEYS]

    log_print("\n⚡ Running Deep Row-Level Hashing and writing discrepancies...")
    total_discrepancies_inserted = run_row_level_validation(
        client, src_ref, tgt_ref, audit_ref, common_cols_to_hash, run_timestamp
    )
    log_print(f"   ✔️ Processing Completed. Logged {total_discrepancies_inserted:,} mismatch records directly to `{audit_ref.dataset_id}.{audit_ref.table_id}`")

    # Final summary display
    log_print("\n=========================================================")
    log_print("📋 VALIDATION RUN SUMMARY")
    log_print("=========================================================")
    log_print(f"  📅 Execution Time     : {run_timestamp.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
    log_print(f"  📐 Schema Drift       : {'Mismatches Found' if schema_mismatches else 'None (Clean)'}")
    log_print(f"  👥 Duplicates (Src)   : {src_dups:,}")
    log_print(f"  👥 Duplicates (Tgt)   : {tgt_dups:,}")
    log_print(f"  ❌ Total Discrepancies: {total_discrepancies_inserted:,}")
    log_print(f"  📝 Audit Location     : `{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE_NAME}`")
    log_print("=========================================================\n")

    # Step 5: Pull exact row discrepancies details from BQ strictly matching this run
    if total_discrepancies_inserted > 0:
        fetch_and_print_mismatch_details(client, audit_ref, run_timestamp, log_print, limit=10)

    # =====================================================================
    # 4. UPLOAD LOG FILE TO GCS
    # =====================================================================
    log_print(f"💾 Uploading log output file directly to GCS...")
    try:
        gcs_hook = GCSHook()
        results_content = "\n".join(log_buffer)
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_NAME,
            object_name=GCS_OBJECT_NAME,
            data=results_content,
            mime_type="text/plain"
        )
        print(f"✔️ DVT_Results.txt updated successfully on GCS: gs://{GCS_BUCKET_NAME}/{GCS_OBJECT_NAME}")
    except Exception as e:
        print(f"❌ Failed to upload results file to GCS: {e}")
        raise e


# =====================================================================
# 5. DAG DEFINITION
# =====================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bq_customer_data_validation",
    default_args=default_args,
    description="End-to-end BigQuery data validation pipeline (Source vs Target)",
    schedule_interval=None,  # Run on-demand (change to cron pattern like "0 6 * * *" for daily runs)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["validation", "bigquery"],
) as dag:

    run_validation = PythonOperator(
        task_id="execute_data_validation",
        python_callable=run_validation_task,
        provide_context=True,
    )