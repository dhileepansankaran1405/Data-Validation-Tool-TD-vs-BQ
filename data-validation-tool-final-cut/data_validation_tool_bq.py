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
AUDIT_TABLE_NAME = "data_validation_mismatches"
# Primary Key Column(s)
PRIMARY_KEYS = ["id"]
# GCS Bucket to Save Results
GCS_BUCKET_NAME = "groovy-scarab-502011-g2-validation-logs"  
GCS_OBJECT_NAME = "data-validation/DVT_Test_Results.txt"

# =====================================================================
# 2. HELPER UTILITIES FOR BIGQUERY
# =====================================================================

def build_pk_json_expr(pks, alias):
    """Builds a dynamic TO_JSON_STRING(JSON_OBJECT()) SQL expression for PKs."""
    pk_elements = ", ".join([f"'{}', {}.`{}`" for pk in pks])
    return f"TO_JSON_STRING(JSON_OBJECT({}))"


def build_hash_expr(columns, alias):
    """Builds a SHA256 HEX row hash of non-PK columns, handling NULL values."""
    concat_terms = [f"COALESCE(CAST({}.`{}` AS STRING), 'NULL')" for col in columns]
    concat_str = ", '||', ".join(concat_terms)
    return f"TO_HEX(SHA256(CONCAT({})))"


def build_struct_expr(columns, alias):
    """Generates STRUCT expression for payload logging."""
    struct_elements = ", ".join([f"{}.`{}`" for col in columns])
    return f"STRUCT({})"


def create_audit_table_if_not_exists(client, audit_ref, log_print):
    """Creates the mismatch tracking audit table if it is not already present."""
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
    """Fetches high-efficiency fast-metadata row counts, falling back to aggregate count on error."""
    try:
        table = client.get_table(table_ref)
        return table.num_rows
    except Exception as e:
        log_print(f"⚠️ Metadata read failed for {table_ref.table_id}. Falling back to standard query. ({})")
        query = f"SELECT COUNT(1) as cnt FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`"
        query_job = client.query(query)
        res = list(query_job.result())
        return res[0].cnt if res else 0


def get_duplicate_count(client, table_ref, pks, log_print):
    """Calculates non-unique key occurrences in target tables."""
    pk_cols = ", ".join([f"`{}`" for pk in pks])
    query = f"""
    SELECT SUM(cnt - 1) as dup_count
    FROM (
      SELECT {}, COUNT(1) as cnt
      FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`
      GROUP BY {}
      HAVING cnt > 1
    )
    """
    try:
        query_job = client.query(query)
        result = list(query_job.result())
        count = result[0].dup_count
        return count if count is not None else 0
    except Exception as e:
        log_print(f"❌ Failed to run duplicate check on {table_ref.table_id}: {}")
        return -1


def validate_schemas(client, src_ref, tgt_ref):
    """Runs structural checks mapping column names and datatypes between systems."""
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
    """Identifies the list of intersecting columns with matching datatypes."""
    query = f"""
    SELECT s.column_name 
    FROM `{src_ref.project}.{src_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS` s
    JOIN `{tgt_ref.project}.{tgt_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS` t
      ON s.column_name = t.column_name AND s.data_type = t.data_type
    WHERE s.table_name = '{src_ref.table_id}' AND t.table_name = '{tgt_ref.table_id}'
    """
    query_job = client.query(query)
    return [row.column_name for row in query_job.result()]


def get_column_types(client, src_ref, common_cols):
    """Queries BigQuery schema catalogs to acquire column types for common fields."""
    cols_str = ", ".join([f"'{}'" for col in common_cols])
    query = f"""
    SELECT column_name, data_type
    FROM `{src_ref.project}.{src_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{src_ref.table_id}' AND column_name IN ({cols_str})
    """
    query_job = client.query(query)
    return {row.column_name: row.data_type for row in query_job.result()}


# =====================================================================
# 3. COLUMN-LEVEL VALIDATION FUNCTION
# =====================================================================
def run_column_validation(client, src_ref, tgt_ref, common_cols, log_print):
    """
    Computes statistical column aggregates (SUM, AVG, MIN, MAX, Nulls)
    to spot disparities between corresponding columns.
    """
    if not common_cols:
        log_print("   ⚠️ No common columns found to perform column validation.")
        return []

    col_types = get_column_types(client, src_ref, common_cols)
    
    agg_exprs = []
    agg_mappings = []  # Tuples mapping (col, aggregation_metric_name, select_alias)

    for col in common_cols:
        dtype = col_types.get(col, "").upper()
        # Clean col name for safe alias usage in BQ query
        safe_col = col.replace(" ", "_")
        
        # All columns get null checks
        agg_exprs.append(f"COUNTIF(`{}` IS NULL) AS {safe_col}__null_cnt")
        agg_mappings.append((col, "NULL_COUNT", f"{safe_col}__null_cnt"))

        # Numeric aggregations (SUM, AVG, MIN, MAX)
        is_numeric = any(t in dtype for t in ["INT", "FLOAT", "NUMERIC", "DECIMAL", "REAL", "DOUBLE"])
        if is_numeric:
            agg_exprs.append(f"SUM(`{}`) AS {safe_col}__sum")
            agg_exprs.append(f"AVG(`{}`) AS {safe_col}__avg")
            agg_exprs.append(f"MIN(`{}`) AS {safe_col}__min")
            agg_exprs.append(f"MAX(`{}`) AS {safe_col}__max")
            agg_mappings.append((col, "SUM", f"{safe_col}__sum"))
            agg_mappings.append((col, "AVG", f"{safe_col}__avg"))
            agg_mappings.append((col, "MIN", f"{safe_col}__min"))
            agg_mappings.append((col, "MAX", f"{safe_col}__max"))
        # Character and Date/Time aggregations (MIN, MAX)
        elif any(t in dtype for t in ["CHAR", "STRING", "DATE", "TIME", "TIMESTAMP"]):
            agg_exprs.append(f"MIN(`{}`) AS {safe_col}__min")
            agg_exprs.append(f"MAX(`{}`) AS {safe_col}__max")
            agg_mappings.append((col, "MIN", f"{safe_col}__min"))
            agg_mappings.append((col, "MAX", f"{safe_col}__max"))

    if not agg_exprs:
        log_print("   ⚠️ No aggregatable columns identified.")
        return []

    select_clause = ",\n      ".join(agg_exprs)
    src_query = f"SELECT {select_clause} FROM `{src_ref.project}.{src_ref.dataset_id}.{src_ref.table_id}`"
    tgt_query = f"SELECT {select_clause} FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.{tgt_ref.table_id}`"

    log_print("   📊 Running aggregate queries on Source and Target...")
    try:
        src_results = list(client.query(src_query).result())[0]
        tgt_results = list(client.query(tgt_query).result())[0]
    except Exception as e:
        log_print(f"   ❌ Failed to execute aggregates for column validation: {}")
        return []

    mismatches = []
    log_print("\n   🔍 COLUMN METRIC DRIFT ANALYSIS (Source vs Target):")
    for col, metric, alias in agg_mappings:
        src_val = src_results[alias]
        tgt_val = tgt_results[alias]
        
        # Check equality with floating point tolerance
        if src_val is None and tgt_val is None:
            match = True
        elif src_val is None or tgt_val is None:
            match = False
        elif isinstance(src_val, (int, float)):
            match = abs(src_val - tgt_val) < 1e-6
        else:
            match = src_val == tgt_val

        status_icon = "✔️" if match else "❌"
        log_print(f"      {status_icon} Column: '{}' | Metric: {metric:10} | Source: {src_val} | Target: {tgt_val}")
        
        if not match:
            mismatches.append({
                "column": col,
                "metric": metric,
                "source_value": src_val,
                "target_value": tgt_val
            })
            
    return mismatches


# =====================================================================
# 4. ROW-LEVEL VALIDATION
# =====================================================================
def run_row_level_validation(client, src_ref, tgt_ref, audit_ref, common_cols, run_timestamp):
    """Executes comparative hash analysis joining both platforms directly in BigQuery."""
    pk_json_src = build_pk_json_expr(PRIMARY_KEYS, "s")
    pk_json_tgt = build_pk_json_expr(PRIMARY_KEYS, "t")
    hash_expr_src = build_hash_expr(common_cols, "s")
    hash_expr_tgt = build_hash_expr(common_cols, "t")
    struct_src = build_struct_expr(common_cols, "s")
    struct_tgt = build_struct_expr(common_cols, "t")
    
    join_cond = " AND ".join([f"s.`{}` = t.`{}`" for pk in PRIMARY_KEYS])
    pk_condition = " OR ".join([f"s.`{}` IS NOT NULL" for pk in PRIMARY_KEYS])

    pk_json_out = f"CASE WHEN {} THEN {} ELSE {} END"

    query = f"""
    INSERT INTO `{audit_ref.project}.{audit_ref.dataset_id}.{audit_ref.table_id}` 
      (validation_time, primary_key_value, mismatch_type, source_row_hash, target_row_hash, details)
    WITH src_data AS (
        SELECT 
            {} AS pk_val,
            {} AS row_hash,
            {} AS row_data,
            *
        FROM `{src_ref.project}.{src_ref.dataset_id}.{src_ref.table_id}` s
    ),
    tgt_data AS (
        SELECT 
            {} AS pk_val,
            {} AS row_hash,
            {} AS row_data,
            *
        FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.{tgt_ref.table_id}` t
    )
    SELECT
        @run_timestamp AS validation_time,
        {} AS primary_key_value,
        CASE
            WHEN s.pk_val IS NULL THEN 'MISSING_IN_SOURCE'
            WHEN t.pk_val IS NULL THEN 'MISSING_IN_TARGET'
            WHEN s.row_hash != t.row_hash THEN 'HASH_MISMATCH'
        END AS mismatch_type,
        s.row_hash AS source_row_hash,
        t.row_hash AS target_row_hash,
        TO_JSON_STRING(STRUCT(s.row_data AS source_record, t.row_data AS target_record)) AS details
    FROM src_data s
    FULL OUTER JOIN tgt_data t ON {}
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
    """Pulls explicit rows showing hash inconsistencies recorded during the runtime cycle."""
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
    LIMIT {}
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
            log_print(f"\n👉 RECORD #{}")
            log_print(f"  🔑 Primary Key Value : {row.primary_key_value}")
            log_print(f"  ❌ Mismatch Type     : {row.mismatch_type}")
            log_print(f"  🌐 Source Hash       : {row.source_row_hash}")
            log_print(f"  🌐 Target Hash       : {row.target_row_hash}")
            log_print(f"  📦 Record Payloads (JSON Structs):")
            log_print(f"     {row.details}")
        log_print("=========================================================\n")
    except Exception as e:
        log_print(f"⚠️ Could not pull detailed row metadata: {}")

# =====================================================================
# 5. CORE VALIDATION ORCHESTRATOR FOR AIRFLOW
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

    src_ref = bigquery.TableReference.from_string(f"{}.{}.{}")
    tgt_ref = bigquery.TableReference.from_string(f"{}.{}.{}")
    audit_ref = bigquery.TableReference.from_string(f"{}.{}.{}")

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

    # Step 4: Core Validation Setup
    log_print("\n🔗 Correlating Common Comparison Columns...")
    common_cols = get_common_columns(client, src_ref, tgt_ref)
    log_print(f"   Intersecting columns evaluated for hash logic: {len(common_cols)} columns.")
    common_cols_to_hash = [c for c in common_cols if c not in PRIMARY_KEYS]

    # Step 4.5: Column-Level Metric Validation
    log_print("\n📊 Running Column-Level Metric Aggregation checks (Sum, Avg, Min, Max, Nulls)...")
    column_mismatches = run_column_validation(client, src_ref, tgt_ref, common_cols_to_hash, log_print)

    # Step 4.6: Deep Row-Level Hashing Check
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
    log_print(f"  📊 Column Agg Drift   : {'Mismatches Found' if column_mismatches else 'None (Clean)'}")
    log_print(f"  ❌ Total Discrepancies: {total_discrepancies_inserted:,}")
    log_print(f"  📝 Audit Location     : `{audit_ref.project}.{audit_ref.dataset_id}.{audit_ref.table_id}`")
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
        print(f"✔️ DVT_Results.txt updated successfully on GCS: gs://{}/{}")
    except Exception as e:
        print(f"❌ Failed to upload results file to GCS: {}")
        raise e

# =====================================================================
# 6. DAG DEFINITION
# =====================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_validation_tool",
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
