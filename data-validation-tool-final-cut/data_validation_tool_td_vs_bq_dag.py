import yaml
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.teradata.hooks.teradata import TeradataHook
from google.cloud import bigquery

# Path to the YAML file produced by the generator script
CONFIG_PATH = "/tmp/validation_config.yaml"


def load_config(config_path):
    """Loads configuration dynamically from a YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to load validation YAML config at {config_path}: {e}")


# =====================================================================
# HELPER UTILITIES
# =====================================================================

def build_pk_json_expr_bq(pks, alias):
    pk_elements = ", ".join([f"'{pk}', {alias}.`{pk}`" for pk in pks])
    return f"TO_JSON_STRING(JSON_OBJECT({pk_elements}))"


def build_hash_expr_td(columns):
    concat_terms = [f"COALESCE(CAST(`{col}` AS VARCHAR(1000)), 'NULL')" for col in columns]
    concat_str = " || '||' || ".join(concat_terms)
    return f"CAST(HASHROW({concat_str}) AS VARCHAR(100))"


def build_hash_expr_bq(columns, alias):
    concat_terms = [f"COALESCE(CAST({alias}.`{col}` AS STRING), 'NULL')" for col in columns]
    concat_str = ", '||', ".join(concat_terms)
    return f"TO_HEX(SHA256(CONCAT({concat_str})))"


def create_audit_table_if_not_exists(bq_client, audit_ref, log_print):
    schema = [
        bigquery.SchemaField("validation_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("primary_key_value", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mismatch_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_row_hash", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("target_row_hash", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("details", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(audit_ref, schema=schema)
    bq_client.create_table(table, exists_ok=True)


# --- Fetch Metadata ---

def get_teradata_row_count(td_hook, db_name, table_name):
    records = td_hook.get_records(f"SELECT COUNT(*) FROM {db_name}.{table_name};")
    return records[0][0] if records else 0


def get_teradata_duplicate_count(td_hook, db_name, table_name, pks):
    pk_cols = ", ".join([f"`{pk}`" for pk in pks])
    sql = f"""
    SELECT SUM(cnt - 1) 
    FROM (
      SELECT {pk_cols}, COUNT(*) as cnt
      FROM {db_name}.{table_name}
      GROUP BY {pk_cols}
      HAVING COUNT(*) > 1
    ) t;
    """
    try:
        records = td_hook.get_records(sql)
        res = records[0][0]
        return res if res is not None else 0
    except Exception:
        return -1


def get_teradata_columns(td_hook, db_name, table_name):
    sql = f"""
    SELECT ColumnTN as column_name, ColumnType as data_type
    FROM DBC.ColumnsV
    WHERE DatabaseName = '{db_name}' AND TableName = '{table_name}';
    """
    try:
        records = td_hook.get_records(sql)
        return {row[0].lower(): row[1].strip() for row in records}
    except Exception:
        sql = f"""
        SELECT ColumnName as column_name, ColumnType as data_type
        FROM DBC.ColumnsV
        WHERE DatabaseName = '{db_name}' AND TableName = '{table_name}';
        """
        records = td_hook.get_records(sql)
        return {row[0].lower(): row[1].strip() for row in records}


def get_bq_metadata_row_count(bq_client, tgt_ref):
    try:
        table = bq_client.get_table(tgt_ref)
        return table.num_rows
    except Exception:
        query = f"SELECT COUNT(1) as cnt FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.{tgt_ref.table_id}`"
        res = list(bq_client.query(query).result())
        return res[0].cnt if res else 0


def get_bq_duplicate_count(bq_client, tgt_ref, pks):
    pk_cols = ", ".join([f"`{pk}`" for pk in pks])
    query = f"""
    SELECT SUM(cnt - 1) as dup_count
    FROM (
      SELECT {pk_cols}, COUNT(1) as cnt
      FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.{tgt_ref.table_id}`
      GROUP BY {pk_cols}
      HAVING cnt > 1
    )
    """
    try:
        result = list(bq_client.query(query).result())
        count = result[0].dup_count
        return count if count is not None else 0
    except Exception:
        return -1


def get_bq_columns(bq_client, tgt_ref):
    query = f"""
    SELECT column_name, data_type
    FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{tgt_ref.table_id}'
    """
    results = bq_client.query(query).result()
    return {row.column_name.lower(): row.data_type for row in results}


# =====================================================================
# ROW LEVEL VALIDATION
# =====================================================================

def run_row_level_validation(td_hook, bq_client, src_db, src_table, tgt_ref, audit_ref, primary_keys, common_cols, run_timestamp, log_print):
    pk_cols_str = ", ".join(primary_keys)
    td_hash_expr = build_hash_expr_td(common_cols)
    
    td_query = f"""
    SELECT {pk_cols_str}, {td_hash_expr} AS src_hash
    FROM {src_db}.{src_table}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk_cols_str} ORDER BY {primary_keys[0]}) = 1;
    """
    log_print("   📡 Querying Teradata source hashes...")
    td_records = td_hook.get_records(td_query)
    td_hashes = {str(row[0]): str(row[1]) for row in td_records}
    
    bq_hash_expr = build_hash_expr_bq(common_cols, "t")
    bq_pk_json = build_pk_json_expr_bq(primary_keys, "t")
    
    bq_query = f"""
    SELECT 
        CAST(t.`{primary_keys[0]}` AS STRING) as pk_val,
        {bq_hash_expr} as tgt_hash,
        {bq_pk_json} as pk_json
    FROM `{tgt_ref.project}.{tgt_ref.dataset_id}.{tgt_ref.table_id}` t
    QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk_cols_str}) = 1
    """
    log_print("   ⚡ Fetching BigQuery target hashes...")
    bq_results = list(bq_client.query(bq_query).result())
    
    audit_rows = []
    bq_pks_seen = set()

    for row in bq_results:
        pk_val = row.pk_val
        bq_pks_seen.add(pk_val)
        tgt_hash = row.tgt_hash
        src_hash = td_hashes.get(pk_val)

        mismatch_type = None
        if src_hash is None:
            mismatch_type = "MISSING_IN_SOURCE"
        elif src_hash != tgt_hash:
            mismatch_type = "HASH_MISMATCH"

        if mismatch_type:
            audit_rows.append({
                "validation_time": run_timestamp.isoformat(),
                "primary_key_value": row.pk_json,
                "mismatch_type": mismatch_type,
                "source_row_hash": src_hash,
                "target_row_hash": tgt_hash,
                "details": f'{{"pk": "{pk_val}", "status": "{mismatch_type}"}}'
            })

    for td_pk, td_hash in td_hashes.items():
        if td_pk not in bq_pks_seen:
            audit_rows.append({
                "validation_time": run_timestamp.isoformat(),
                "primary_key_value": f'{{"{primary_keys[0]}": "{td_pk}"}}',
                "mismatch_type": "MISSING_IN_TARGET",
                "source_row_hash": td_hash,
                "target_row_hash": None,
                "details": f'{{"pk": "{td_pk}", "status": "MISSING_IN_TARGET"}}'
            })

    if audit_rows:
        errors = bq_client.insert_rows_json(
            f"{audit_ref.project}.{audit_ref.dataset_id}.{audit_ref.table_id}",
            audit_rows
        )
        if errors:
            log_print(f"⚠️ Errors inserting audit rows to BQ: {errors}")

    return len(audit_rows)


# =====================================================================
# CORE ORCHESTRATOR
# =====================================================================

def run_validation_task(**kwargs):
    # Load YAML Configuration
    cfg = load_config(CONFIG_PATH)

    project_id = cfg["project_id"]
    td_conn_id = cfg["teradata_conn_id"]
    gcs_bucket = cfg["gcs"]["bucket_name"]
    gcs_object = cfg["gcs"]["object_name"]
    audit_cfg = cfg["audit"]

    bq_client = bigquery.Client(project=project_id)
    td_hook = TeradataHook(teradata_conn_id=td_conn_id)
    run_timestamp = datetime.now(timezone.utc)
    
    log_buffer = []

    def log_print(msg):
        print(msg)
        log_buffer.append(msg)

    log_print("🚀 Initializing Teradata to BigQuery Validation Pipeline from YAML Config...")

    audit_ref = bigquery.TableReference.from_string(
        f"{project_id}.{audit_cfg['dataset']}.{audit_cfg['table_name']}"
    )
    create_audit_table_if_not_exists(bq_client, audit_ref, log_print)

    # Loop over every table configuration defined in YAML
    for table_pair in cfg.get("tables", []):
        src_db = table_pair["source"]["database"]
        src_table = table_pair["source"]["table_name"]
        tgt_ds = table_pair["target"]["dataset"]
        tgt_table = table_pair["target"]["table_name"]
        primary_keys = table_pair["primary_keys"]

        tgt_ref = bigquery.TableReference.from_string(f"{project_id}.{tgt_ds}.{tgt_table}")

        log_print(f"\n=========================================================")
        log_print(f"🔍 VALIDATING: Teradata ({src_db}.{src_table}) ➡️ BigQuery ({tgt_ds}.{tgt_table})")
        log_print(f"=========================================================")

        # 1. Row Counts
        src_cnt = get_teradata_row_count(td_hook, src_db, src_table)
        tgt_cnt = get_bq_metadata_row_count(bq_client, tgt_ref)
        log_print(f"   Teradata Source Count : {src_cnt:,}")
        log_print(f"   BigQuery Target Count : {tgt_cnt:,}")
        log_print(f"   Count Difference      : {src_cnt - tgt_cnt:,}")

        # 2. Duplicates
        src_dups = get_teradata_duplicate_count(td_hook, src_db, src_table, primary_keys)
        tgt_dups = get_bq_duplicate_count(bq_client, tgt_ref, primary_keys)
        log_print(f"   Teradata Duplicate PK Rows : {src_dups:,}")
        log_print(f"   BigQuery Duplicate PK Rows : {tgt_dups:,}")

        # 3. Columns & Hashes
        src_cols = get_teradata_columns(td_hook, src_db, src_table)
        tgt_cols = get_bq_columns(bq_client, tgt_ref)
        common_cols = [c for c in src_cols.keys() if c in tgt_cols.keys() and c not in primary_keys]

        log_print(f"   Intersecting evaluation columns: {len(common_cols)}")

        # 4. Row Hash Validation
        discrepancies = run_row_level_validation(
            td_hook, bq_client, src_db, src_table,
            tgt_ref, audit_ref, primary_keys, common_cols, run_timestamp, log_print
        )
        log_print(f"   ✔️ Discrepancies logged to BigQuery: {discrepancies:,}")

    # Log Upload to GCS
    try:
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=gcs_object,
            data="\n".join(log_buffer),
            mime_type="text/plain"
        )
        print(f"✔️ Results saved: gs://{gcs_bucket}/{gcs_object}")
    except Exception as e:
        print(f"❌ Failed to upload log file to GCS: {e}")
        raise e


# =====================================================================
# AIRFLOW DAG DEFINITION
# =====================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="teradata_to_bq_yaml_validation",
    default_args=default_args,
    description="YAML Config-driven Teradata to BigQuery Data Validation Pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["validation", "yaml", "teradata", "bigquery"],
) as dag:

    run_validation = PythonOperator(
        task_id="execute_yaml_data_validation",
        python_callable=run_validation_task,
        provide_context=True,
    )