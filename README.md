# Data-Validation-Tool-TD-vs-BQ 

**Author:** Dhileepan Sankaran 

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

Flow chart Overview

<img width="975" height="650" alt="image" src="https://github.com/user-attachments/assets/0054e2c7-9c1a-4781-af89-273974e3ca28" />

                                                
**8-Step Implementation Workflow**

1.	**Secret Management:** Securely store Teradata passwords in Google Secret Manager to avoid hardcoded credentials.
2.	**Audit Setup:** Initialize a BigQuery audit_dataset to act as the central repository for validation telemetry.
3.	**Containerization:** Build a Docker image containing the DVT library and Teradata JDBC drivers and push to Artifact Registry.
4.	**Inventory Mapping:** Maintain migration_inventory.csv to map source/target tables and define test types.
5.	**Config Generation:** Use a Python script to automatically generate YAML configuration files for every table in the inventory.
6.	**Staging:** Sync YAML files to the Cloud Composer GCS bucket for access by the Airflow DAG.
7.	**Dynamic Orchestration:** Deploy an Airflow DAG that scans the GCS bucket and triggers parallel GKE Pods for each validation task.
8.	**Analysis:** Monitor execution through the Airflow UI and query the dvt_execution_logs in BigQuery to identify discrepancies like row-count mismatches or data truncation.

**Step-by-Step Migration Validation Workflow**

•	**Step 1**: **Set Up Secret Manager**

o	Enable Google Secret Manager API.

o	Create a secret container named Teradata-migration-password.

o	Upload your raw Teradata database password securely as a version string.

•	**Step 2: Create the BigQuery Logging Destination**

o	Create a dedicated BigQuery dataset named audit_dataset via Cloud Shell.

o	This provides a destination for DVT to write its test execution histories automatically.

**•	Step 3: Build & Push the DVT Container Image**

o	Create minimal Dockerfile installing Python, system dependencies, and the google-pyspark-data-validation[bigquery,teradata] library.

o	Run a shell deployment script to compile the container image and upload it to a Google Cloud Artifact Registry repository.

**•	Step 4: Maintain the Table Inventory Spreadsheet**

o	Create a master migration_inventory.csv metadata file.

o	Map out your source tables, target datasets, test types (row or column), primary key constraints, and any date partitioning filter rules.

**•	Step 5: Generate Configuration Rules Automaticall**y

o	Execute the Python generation script locally.

o	This script fetches the database password from Secret Manager, parse the CSV map, and generates separate, customized validation .yaml files for every single table.

**•	Step 6: Deploy Configuration Blueprints to Storage**

o	Upload your newly created directory of .yaml definition rules to your Cloud Composer's designated Google Cloud Storage (GCS) bucket path using gsutil.

**•	Step 7: Deploy the Dynamic Airflow DAG**

o	Upload the orchestration DAG into your Airflow /dags directory.

o	This workflow dynamically scans the GCS folder and runs a high-performance KubernetesPodOperator task for each table in parallel.

**•	Step 8: Monitor and Analyze Results**

o	Trigger the workflow via the Airflow dashboard interface.

o	Query the audit_dataset.dvt_execution_logs table inside BigQuery to immediately surface passing metrics or specific row-count variations.        



