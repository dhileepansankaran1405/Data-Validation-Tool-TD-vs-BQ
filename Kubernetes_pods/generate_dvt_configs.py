from datetime import datetime, timedelta
import csv
import os
import yaml

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.log.secrets_masker import mask_secret
from google.cloud import secretmanager

# System Parameters
PROJECT_ID = "groovy-scarab-502011-g2"
SECRET_ID = "teradata-migration-password"
TERADATA_HOST = "10.10.20.5"
TERADATA_USER = "migration_audit_user"

CSV_FILE_PATH = "/opt/airflow/dags/data/migration_inventory.csv"  # Update path as needed
OUTPUT_DIR = "/opt/airflow/dags/dvt_configs"                      # Update output path as needed

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="dvt_yaml_configuration_generator",
    default_args=default_args,
    description="Fetches Teradata credentials securely and generates DVT YAML configurations from an inventory CSV.",
    schedule_interval=None,  # Set your schedule here (e.g., '@daily')
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["migration", "teradata", "bigquery", "dvt"],
)
def dvt_config_generator_dag():

    @task()
    def get_secret_password() -> str:
        """Retrieves plain text credentials from GCP Secret Manager and masks it from Airflow logs."""
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            password = response.payload.data.decode("UTF-8")
            
            # Mask password to prevent accidental leakage in Airflow UI/Logs
            mask_secret(password)
            return password
        except Exception as e:
            raise AirflowException(
                f"❌ Secret Manager Error: Unable to fetch secret '{SECRET_ID}'. Details: {e}"
            )

    @task()
    def generate_yaml_files(teradata_pass: str):
        """Processes the CSV file and compiles output YAML configuration profiles."""
        if not os.path.exists(CSV_FILE_PATH):
            raise AirflowException(
                f"❌ Critical Failure: Inventory map '{CSV_FILE_PATH}' cannot be located."
            )

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"🔄 Formulating configuration blueprints into destination: '{OUTPUT_DIR}'...")

        generated_count = 0

        # 'utf-8-sig' handles standard UTF-8 as well as Excel BOM markers
        with open(CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)

            for line_num, row in enumerate(reader, start=2):
                source_table = (row.get("source_table") or "").strip()
                target_table = (row.get("target_table") or "").strip()
                validation_type = (row.get("validation_type") or "").strip().lower()
                primary_key = (row.get("primary_key") or "").strip()
                filter_condition = (row.get("filter_condition") or "").strip()

                if not source_table or not target_table:
                    print(f"⚠️ Line {line_num}: Operational Warning. Missing table details. Skipping.")
                    continue

                validation_entry = {
                    "type": validation_type,
                    "table_name": f"{source_table}={target_table}",
                }

                if validation_type == "row":
                    validation_entry.update({"primary_keys": primary_key, "concat": "*"})
                if filter_condition:
                    validation_entry["filters"] = filter_condition

                yaml_data = {
                    "source": f'{{"source_type":"Teradata","host":"{TERADATA_HOST}","user":"{TERADATA_USER}","password":"{teradata_pass}"}}',
                    "target": f'{{"source_type":"BigQuery","project_id":"{PROJECT_ID}"}}',
                    "result_handler": {
                        "type": "BigQuery",
                        "table_id": f"{PROJECT_ID}.audit_dataset.dvt_execution_logs",
                    },
                    "validations": [validation_entry],
                }

                sanitized_filename = (
                    f"validate_{source_table.replace('.', '_').lower()}.yaml"
                )
                output_filepath = os.path.join(OUTPUT_DIR, sanitized_filename)

                with open(output_filepath, "w", encoding="utf-8") as outfile:
                    yaml.dump(
                        yaml_data, outfile, default_flow_style=False, sort_keys=False
                    )

                generated_count += 1

        print(f"✅ YAML Config Generation complete. Compiled {generated_count} mapping profiles.")
        return generated_count

    # Define task orchestration flow
    secret = get_secret_password()
    generate_yaml_files(secret)


# Instantiate the DAG
dvt_config_generator_dag()