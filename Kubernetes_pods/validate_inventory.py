from datetime import datetime, timedelta
import csv
import os

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# Configuration
CSV_FILE_PATH = "/opt/airflow/dags/data/migration_inventory.csv"  # Update path as needed
VALID_TYPES = {"row", "column"}
EXPECTED_FIELDS = {
    "source_table",
    "target_table",
    "validation_type",
    "primary_key",
    "filter_condition",
}

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="migration_inventory_compliance_check",
    default_args=default_args,
    description="Static analysis compliance check for database migration inventory catalog.",
    schedule_interval=None,  # Set your schedule here (e.g., '@daily', '0 0 * * *')
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["migration", "compliance", "data_quality"],
)
def migration_compliance_dag():

    @task()
    def validate_csv_inventory(file_path: str):
        print(f"🕵️ Starting compliance analysis on catalog map: '{file_path}'...")

        if not os.path.exists(file_path):
            raise AirflowException(
                f"❌ Critical Failure: Master document '{file_path}' cannot be resolved locally."
            )

        errors_detected = 0
        warnings_detected = 0

        # 'utf-8-sig' handles standard UTF-8 as well as Excel BOM markers
        with open(file_path, mode="r", encoding="utf-8-sig") as file:
            try:
                sample = file.read(2048)
                file.seek(0)
                dialect = csv.Sniffer().sniff(sample)
            except Exception:
                file.seek(0)
                dialect = csv.excel

            reader = csv.DictReader(file, dialect=dialect)

            current_headers = {h.strip() for h in (reader.fieldnames or []) if h}
            if not EXPECTED_FIELDS.issubset(current_headers):
                missing = EXPECTED_FIELDS - current_headers
                raise AirflowException(
                    f"❌ Schema Validation Failure: Missing column definitions in CSV header row: {missing}"
                )

            for line_num, row in enumerate(reader, start=2):
                source = (row.get("source_table") or "").strip()
                target = (row.get("target_table") or "").strip()
                v_type = (row.get("validation_type") or "").strip().lower()
                p_key = (row.get("primary_key") or "").strip()
                filter_cond = (row.get("filter_condition") or "").strip()

                if not source or not target:
                    print(
                        f"❌ Row {line_num}: Operational Error. Source or Target naming references are completely missing."
                    )
                    errors_detected += 1
                    continue

                if v_type not in VALID_TYPES:
                    print(
                        f"❌ Row {line_num} [{source}]: Configuration Error. Engine type '{v_type}' is invalid. Supported options: {VALID_TYPES}"
                    )
                    errors_detected += 1

                if v_type == "row" and not p_key:
                    print(
                        f"❌ Row {line_num} [{source}]: Execution Blocked. Row-level comparison checks strictly require a defined primary key."
                    )
                    errors_detected += 1

                if v_type == "column" and p_key:
                    print(
                        f"⚠️ Row {line_num} [{source}]: Optimization Warning. Column evaluations ignore primary key settings."
                    )
                    warnings_detected += 1

                if any(char in filter_cond for char in (";", "--", "/*", "*/")):
                    print(
                        f"❌ Row {line_num} [{source}]: Security Alert. Suspicious SQL formatting sequences inside filter criteria fields."
                    )
                    errors_detected += 1

        print("\n" + "=" * 78)
        print(
            f"📊 HEALTH REPORT SUMMARY: [Errors Isolated: {errors_detected}] | [Warnings Identified: {warnings_detected}]"
        )
        print("=" * 78)

        if errors_detected > 0:
            raise AirflowException(
                f"🛑 Migration build halted. Isolated {errors_detected} inventory configuration problem(s)."
            )

        print("🚀 Inventory checks passed safely! Ready for downstream compilation.")
        return True

    # Instantiate task execution
    validate_csv_inventory(CSV_FILE_PATH)


# Register DAG instance
migration_compliance_dag()