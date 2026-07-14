# Data-Validation-Tool-TD-vs-BQ 

Enterprise Data Validation Framework using Data Validation Tool (DVT) Approach for Teradata vs Google Bigquery

This Enterprise data migration validation framework leverages code repository, reduces manual execution to deploy, scale an automated data validation pipeline, perform data validation & generate metrics report.

•	**Automation**: Replaces manual checks with a code-driven, reproducible pipeline.

•	**Scalability**: Leverages Google Kubernetes Engine (GKE) and Cloud Composer to process hundreds of tables in parallel.

•	**Auditability**: Centralizes all validation results in BigQuery for real-time reporting via Looker Studio.

**Services used:** Google Cloud Data Validation Tool (DVT),  orchestrated dynamically via Cloud Composer (Apache Airflow) using KubernetesPodOperator (KPO) tasks, Looker Studio for analyzing reports or Big Query table for logging.
Architecture & Tech Stack

•	**Column**: Aggregations (Sum, Avg, Min, Max, Counts, and Group By)

•	**Schema**: Checks for data type and structure alignment.

•	**Row-Level**: Cell-to-cell matching via custom queries or Cell-Level Hashing for high-performance fingerprinting on massive datasets.

•	**Custom** Query SQL handling. 

Key Metrics Captured
The framework generates a structured log including run_id, status (PASS/FAIL), source_count, target_count, and difference_count, enabling immediate surface-level identification of migration errors.


**Architecture Diagram:**

<img width="975" height="699" alt="image" src="https://github.com/user-attachments/assets/fcb6fb38-e614-4d3d-a12a-4d302894e991" />

**Inventory CSV** → Config Compiler Script Reads mappings &  generates DVT compliant YAML blueprints.

**Secret Manager** → Compiler Script Supplies secure credentials dynamically at runtime.

**Local DVT YAML Files** → GCS Bucket Synced using gsutil for Cloud Composer access.

**Cloud Composer (Airflow)** → GKE Pod Spins up transient pods to execute validation tasks.

**Teradata** → GKE Pod → BigQuery Performs data validation and loads results.

**BigQuery Audit Table** → Looker Studio Pipes audit metrics for visualization and monitoring.

Stage	Description

Inventory CSV → Compiler Script	Reads mappings and generates YAML configs.

Secret Manager → Compiler Script	Supplies secure credentials dynamically.

Local Configs → GCS Bucket	Uploads YAML blueprints for Composer tasks.

Cloud Composer → Transient Pod	Executes validation jobs using uploaded configs.

Teradata → GKE Pod → BigQuery	Performs data validation and loads results.

BigQuery Audit Table → Looker Studio	Visualizes audit metrics and pipeline health.

