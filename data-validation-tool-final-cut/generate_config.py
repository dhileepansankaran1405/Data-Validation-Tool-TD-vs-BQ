import yaml

config_data = {
    "project_id": "groovy-scarab-502011-g2",
    "teradata_conn_id": "teradata_default",
    "gcs": {
        "bucket_name": "groovy-scarab-502011-g2-validation-logs",
        "object_name": "data-validation/DVT_Test_Results.txt"
    },
    "tables": [
        {
            "source": {
                "database": "Source_Db",
                "table_name": "Customer"
            },
            "target": {
                "dataset": "Finance",
                "table_name": "Customer"
            },
            "primary_keys": ["id"]
        }
        # You can add more table pairs here easily!
    ],
    "audit": {
        "dataset": "Finance",
        "table_name": "data_validation_mismatches"
    }
}

# Write config to YAML file
yaml_file_path = "/tmp/validation_config.yaml"
with open(yaml_file_path, "w") as f:
    yaml.dump(config_data, f, default_flow_style=False)

print(f"✔️ Config file successfully generated at {yaml_file_path}")