import yaml

config_path = "/tmp/validation_config.yaml"

# 1. Load the existing configuration
with open(config_path, "r") as f:
    config_data = yaml.safe_load(f)

# Ensure the 'tables' list exists
if "tables" not in config_data or config_data["tables"] is None:
    config_data["tables"] = []

# 2. Define the new table pair you want to append
new_table_entry = {
    "source": {
        "database": "Source_Db",
        "table_name": "Invoices"
    },
    "target": {
        "dataset": "Finance",
        "table_name": "Invoices"
    },
    "primary_keys": ["invoice_id"]
}

# 3. Append to the tables list
config_data["tables"].append(new_table_entry)

# 4. Save back to the YAML file
with open(config_path, "w") as f:
    yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)

print("✔️ Appended new table pair to validation_config.yaml successfully!")