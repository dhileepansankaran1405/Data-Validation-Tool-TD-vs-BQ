import sys
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# =====================================================================
# 1. CONFIGURATION
# =====================================================================
PROJECT_ID = "data-validation-activity"

# Source Table
SRC_DATASET = "source_dataset_name"  # Replace with actual source dataset
SRC_TABLE_NAME = "source_table_name"  # Replace with actual source table

# Target Table
TGT_DATASET = "target_dataset_name"  # Replace with actual target dataset
TGT_TABLE_NAME = "target_table_name"  # Replace with actual target table

# Mismatch / Audit Table
AUDIT_DATASET = "audit_dataset_name"  # Replace with audit dataset
AUDIT_TABLE_NAME = "validation_mismatches"

# List of Primary Key columns (handles composite/multiple keys)
PRIMARY_KEYS = ["id"]  # e.g., ["id"] or ["order_id", "item_id"]

# =====================================================================
# 2. VALIDATION ORCHESTRATOR
# =====================================================================


def build_pk_json_expr(pks, alias):
    """Generates JSON string of PK structure for easy identification."""
    pk_elements = ", ".join([f"'{pk}', {alias}.`{pk}`" for pk in pks])
    return f"TO_JSON_STRING(JSON_OBJECT({pk_elements}))"


def build_hash_expr(columns, alias):
    """Generates SHA256 expression based on matching columns to perform row comparison."""
    concat_terms = []
    for col in columns:
        concat_terms.append(f"COALESCE(CAST({alias}.`{col}` AS STRING), 'NULL')")
    concat_str = ", '||', ".join(concat_terms)
    return f"TO_HEX(SHA256(CONCAT({concat_str})))"


def build_struct_expr(columns, alias):
    """Builds a STRUCT representation of row data."""
    struct_elements = ", ".join([f"{alias}.`{col}`" for col in columns])
    return f"STRUCT({struct_elements})"


def create_audit_table_if_not_exists(client, audit_ref):
    """Creates the target audit table if it does not already exist."""
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
    print(f"✔️ Audit Mismatch Table verified: {audit_ref.path}")


def get_metadata_row_count(client, table_ref):
    """Fetches high-speed metadata row count (no query cost)."""
    try:
        table = client.get_table(table_ref)
        return table.num_rows
    except Exception as e:
        print(f"⚠️ Metadata read failed for {table_ref.table_id}. Falling back to standard query. ({e})")
        query = f"SELECT COUNT(1) as cnt FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`"
        query_job = client.query(query)
        res = list(query_job.result())
        return res[0].cnt if res else 0


def get_duplicate_count(client, table_ref, pks):
    """Counts non-unique primary keys in a table."""
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
        print(f"❌ Failed to run duplicate check on {table_ref.table_id}: {e}")
        return -1


def validate_schemas(client, src_ref, tgt_ref):
    """Performs schema & column validation checking for type/name compatibility."""
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
    """Returns columns existing in both tables with identical types (used for hashing)."""
    query = f"""
    SELECT s.column_name 
    FROM `{src_ref.project}.{src_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS` s
    JOIN `{tgt_ref.project}.{tgt_ref.dataset_id}.INFORMATION_SCHEMA.COLUMNS` t
      ON s.column_name = t.column_name AND s.data_type = t.data_type
    WHERE s.table_name = '{src_ref.table_id}' AND t.table_name = '{tgt_ref.table_id}'
    """
    query_job = client.query(query)
    return [row.column_name for row in query_job.result()]


def run_row_level_validation(client, src_ref, tgt_ref, audit_ref, common_cols):
    """Runs high-performance hashing query to isolate and log mismatches directly to BigQuery."""
    # Build parts dynamically
    pk_json_src = build_pk_json_expr(PRIMARY_KEYS, "s")
    pk_json_tgt = build_pk_json_expr(PRIMARY_KEYS, "t")
    hash_expr_src = build_hash_expr(common_cols, "s")
    hash_expr_tgt = build_hash_expr(common_cols, "t")
    struct_src = build_struct_expr(common_cols, "s")
    struct_tgt = build_struct_expr(common_cols, "t")
    join_cond = " AND ".join([f"s.`{pk}` = t.`{pk}`" for pk in PRIMARY_KEYS])
    pk_condition = " OR ".join([f"s.`{pk}` IS NOT NULL" for pk in PRIMARY_KEYS])

    # Dynamic outer query
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
        CURRENT_TIMESTAMP() AS validation_time,
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
    
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    return query_job.num_dml_affected_rows


# =====================================================================
# 3. MAIN RUNTIME
# =====================================================================
def main():
    print("🚀 Initializing End-to-End BigQuery Data Validation Pipeline...")
    client = bigquery.Client(project=PROJECT_ID)

    src_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{SRC_DATASET}.{SRC_TABLE_NAME}")
    tgt_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{TGT_DATASET}.{TGT_TABLE_NAME}")
    audit_ref = bigquery.TableReference.from_string(f"{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE_NAME}")

    # Step 0: Set up/Verify Audit Table
    create_mismatch_table_if_not_exists(client, audit_ref)

    # Step 1: Count Validation
    print("\n📊 Running Row Count checks...")
    src_cnt = get_metadata_row_count(client, src_ref)
    tgt_cnt = get_metadata_row_count(client, tgt_ref)
    print(f"   Source Row Count: {src_cnt:,}")
    print(f"   Target Row Count: {tgt_cnt:,}")
    print(f"   Difference      : {src_cnt - tgt_cnt:,}")

    # Step 2: Duplicate Validation
    print("\n🔍 Running Duplicate PK checks...")
    src_dups = get_duplicate_count(client, src_ref, PRIMARY_KEYS)
    tgt_dups = get_duplicate_count(client, tgt_ref, PRIMARY_KEYS)
    print(f"   Source Duplicate PK Rows: {src_dups:,}")
    print(f"   Target Duplicate PK Rows: {tgt_dups:,}")

    # Step 3: Column / Schema Validation
    print("\n📐 Running Schema / Column Mismatch checks...")
    schema_mismatches = validate_schemas(client, src_ref, tgt_ref)
    if schema_mismatches:
        print("   ❌ SCHEMA MISMATCHES FOUND:")
        for row in schema_mismatches:
            print(f"      - Column: '{row.column_name}' | Source Type: {row.source_type} | Target Type: {row.target_type} | Status: {row.status}")
    else:
        print("   ✔️ Schemas Match Perfectly.")

    # Step 4: Row-to-Row Hash Validation
    print("\n🔗 Correlating Common Comparison Columns...")
    common_cols = get_common_columns(client, src_ref, tgt_ref)
    print(f"   Intersecting columns for hash logic: {len(common_cols)} columns evaluated.")

    # Remove Primary Keys from dynamic Hashing set to avoid duplication
    common_cols_to_hash = [c for c in common_cols if c not in PRIMARY_KEYS]

    print("\n⚡ Running Deep Row-Level Hashing and populating discrepancies...")
    total_discrepancies_inserted = run_row_level_validation(
        client, src_ref, tgt_ref, audit_ref, common_cols_to_hash
    )
    print(f"   ✔️ Completed. Logged {total_discrepancies_inserted:,} discrepancies into `{audit_ref.dataset_id}.{audit_ref.table_id}`")

    # Final summary display
    print("\n=========================================================")
    print("📋 VALIDATION RUN COMPLETE")
    print("=========================================================")
    print(f"  📅 Execution Time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  📐 Schema Drift       : {'Mismatches Found' if schema_mismatches else 'None (Clean)'}")
    print(f"  👥 Duplicates (Src)   : {src_dups:,}")
    print(f"  👥 Duplicates (Tgt)   : {tgt_dups:,}")
    print(f"  ❌ Total Discrepancies: {total_discrepancies_inserted:,}")
    print(f"  📝 Audit Location     : `{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE_NAME}`")
    print("=========================================================\n")


if __name__ == "__main__":
    main()
