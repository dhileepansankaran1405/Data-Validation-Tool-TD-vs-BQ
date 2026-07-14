Initialize Bigquery Audit Logging Store
The Data Validation Tool outputs machine-readable validation logs. Instead of letting execution results print as unstructured text inside Airflow logs, DVT pipes metrics straight back to a central BigQuery dataset. This allows engineering teams to track and query historical migration states.
Detailed Actions:
1.	Inside your active Cloud Shell instance, execute the following BigQuery command:
Bash command
bq mk --location=us-central1 audit_dataset
2.	Note: We do not need to create columns or define schemas by manually. DVT automatically maps out, declares, and builds the internal layout structures of the target tracking database (dvt_execution_logs) when the initial Airflow verification pod connects and fires for the first time.
