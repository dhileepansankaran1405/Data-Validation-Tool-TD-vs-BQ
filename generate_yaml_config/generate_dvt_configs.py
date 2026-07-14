import csv
import os
import yaml
from google.cloud import secretmanager

# System Parameters
PROJECT_ID = "your-gcp-project-id"   # ⚠️ MODIFY WITH YOUR ACTUAL GCP PROJECT ID
SECRET_ID = "teradata-migration-password"
TERADATA_HOST = "10.10.20.5"         # ⚠️ MODIFY WITH YOUR SOURCE DATALAKE HOST IP
TERADATA_USER = "migration_audit_user"

CSV_FILE = "migration_inventory.csv"
OUTPUT_DIR = "dvt_configs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_secret_password():
    """Retrieves plain text credentials from Secret Manager securely at execution runtime."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")

def generate_yaml():
    teradata_pass = get_secret_password()
    print(f"🔄 Formulating configuration blueprints...")
    
    with open(CSV_FILE, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        generated_count = 0
        
        for row in reader:
            source_table = row['source_table'].strip()
            target_table = row['target_table'].strip()
            validation_type = row['validation_type'].strip().lower()
            primary_key = row['primary_key'].strip()
            filter_condition = row['filter_condition'].strip()

            if not source_table or not target_table: 
                continue

            validation_entry = {
                "type": validation_type,
                "table_name": f"{source_table}={target_table}"
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
                    "table_id": f"{PROJECT_ID}.audit_dataset.dvt_execution_logs"
                },
                "validations": [validation_entry]
            }

            sanitized_filename = f"validate_{source_table.replace('.', '_').lower()}.yaml"
            output_filepath = os.path.join(OUTPUT_DIR, sanitized_filename)

            with open(output_filepath, 'w', encoding='utf-8') as outfile:
                yaml.dump(yaml_data, outfile, default_flow_style=False, sort_keys=False)
            
            generated_count += 1
            
    print(f"✅ YAML Config Generation complete. Compiled {generated_count} mapping profiles.")

if __name__ == "__main__":
    generate_yaml()
