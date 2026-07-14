from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# ==========================================
# CONFIGURATION MATRIX (Metadata Inventory)
# ==========================================
# Supports composite primary keys (comma-separated strings) and dynamic partitioning boundaries.
TABLES_TO_VALIDATE = [
    {
        "source_table": "Finance.Transactions",
        "target_table": "finance_dataset.transactions",
        "primary_key": "transaction_id,location_id", # Supported Composite Keys
        "filter_condition": "transaction_date >= '2026-01-01'", 
        "validation_type": "row" # Handled via row hash comparisons
    },
    {
        "source_table": "RETAIL.CUSTOMER_DIM",
        "target_table": "retail_dataset.customer_dim",
        "primary_key": "customer_id",
        "filter_condition": "1=1",  
        "validation_type": "column" # Handled via aggregate math overrides
    }
]

# Central environment and image management
DVT_IMAGE = "us-central1-docker.pkg.dev/your-gcp-project/dvt-repo/data-validation:latest"
GCP_CONN_PROJECT = "your-gcp-project-id"

# Connection JSON blocks passed inline to the DVT engine
TERADATA_CONN_STRING = '{"source_type":"Teradata","host":"10.x.x.x","user":"migration_user","password":"SecurePassword"}'
BIGQUERY_CONN_STRING = f'{{"source_type":"BigQuery","project_id":"{GCP_CONN_PROJECT}"}}'

default_args = {
    'owner': 'data-migration-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email': ['migration-alerts@yourcompany.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'enterprise_teradata_bq_validation_scale',
    default_args=default_args,
    description='Dynamic scaling framework for Teradata to BigQuery resilient validations via KPO',
    schedule_interval=None,  
    catchup=False,
    max_active_runs=3,        
) as dag:

    # Loop through the inventory matrix to dynamically build dynamic execution pipelines
    for table_config in TABLES_TO_VALIDATE:
        
        # Format task IDs to safely match structural Kubernetes DNS standards
        sanitized_task_id = table_config["source_table"].replace(".", "-").replace("_", "-").lower()
        
        # Base string formatting for target logging destinations 
        summary_metrics_table = f"{GCP_CONN_PROJECT}.audit_dataset.summary_metrics"
        row_discrepancies_table = f"{GCP_CONN_PROJECT}.audit_dataset.row_discrepancies"

        # ---------------------------------------------------------------------
        # VALIDATION LOGIC 1: Row Hash Validation 
        # ---------------------------------------------------------------------
        if table_config["validation_type"] == "row":
            # Formulating raw execution string to run via sh interpreter inside the pod container.
            # This enables using '|| true' to bypass non-zero exit codes when data mismatches occur.
            raw_dvt_script = (
                f"data-validation validate row "
                f"-sc '{TERADATA_CONN_STRING}' "
                f"-tc '{BIGQUERY_CONN_STRING}' "
                f"-tbls '{table_config['source_table']}={table_config['target_table']}' "
                f"--primary-keys '{table_config['primary_key']}' "
                f"--concat '*' "
                f"--use-hash "
                f"--filters \"{table_config['filter_condition']}\" "
                f"-bqrs '{summary_metrics_table}' "
                f"-rvrt '{row_discrepancies_table}' "
                f"|| true"
            )
            
        # ---------------------------------------------------------------------
        # VALIDATION LOGIC 2: Column Aggregation Validation 
        # ---------------------------------------------------------------------
        else:
            raw_dvt_script = (
                f"data-validation validate column "
                f"-sc '{TERADATA_CONN_STRING}' "
                f"-tc '{BIGQUERY_CONN_STRING}' "
                f"-tbls '{table_config['source_table']}={table_config['target_table']}' "
                f"--filters \"{table_config['filter_condition']}\" "
                f"-bqrs '{summary_metrics_table}' "
                f"|| true"
            )

        # ---------------------------------------------------------------------
        # KUBERNETES TRANSITIONAL POD OPERATOR INTERFACE
        # ---------------------------------------------------------------------
        run_dvt_validation = KubernetesPodOperator(
            task_id=f"validate-{sanitized_task_id}",
            name=f"dvt-{sanitized_task_id}",
            namespace="default",
            image=DVT_IMAGE,
            # Force container execution into an interactive shell to parse compound commands and '|| true' redirects
            cmds=["sh", "-c"],
            arguments=[raw_dvt_script],
            
            # Resource limits to control resource usage on the GKE cluster
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "1", "memory": "2Gi"},
                limits={"cpu": "2", "memory": "4Gi"}
            ),
            # Wipes pod infrastructure context post-execution to optimize available GKE worker nodes
            is_delete_operator_pod=True,
            get_logs=True
        )

        # Instantiates tasks dynamically in parallel. Airflow automatically coordinates horizontal 
        # GKE scaling to address inventory size.
        run_dvt_validation

