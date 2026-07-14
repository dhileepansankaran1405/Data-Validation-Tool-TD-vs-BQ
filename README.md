# Data-Validation-Tool-TD-vs-BQ 
Enterprise Data Validation Framework using Data Validation Tool (DVT) Approach for Teradata vs Google Bigquery

This Enterprise data migration validation framework leverages code repository, reduces manual execution to deploy, scale an automated data validation pipeline, perform data validation & generate metrics report.

•	Automation: Replaces manual checks with a code-driven, reproducible pipeline.
•	Scalability: Leverages Google Kubernetes Engine (GKE) and Cloud Composer to process hundreds of tables in parallel.
•	Auditability: Centralizes all validation results in BigQuery for real-time reporting via Looker Studio.

Services used: Google Cloud Data Validation Tool (DVT),  orchestrated dynamically via Cloud Composer (Apache Airflow) using KubernetesPodOperator (KPO) tasks, Looker Studio for analyzing reports or Big Query table for logging.
Architecture & Tech Stack

•	Column: Aggregations (Sum, Avg, Min, Max), Counts, and Group By.
•	Schema: Checks for data type and structure alignment.
•	Row-Level: Cell-to-cell matching via custom queries or Cell-Level Hashing for high-performance fingerprinting on massive datasets.
•	Custom Query SQL handling. 

Key Metrics Captured
The framework generates a structured log including run_id, status (PASS/FAIL), source_count, target_count, and difference_count, enabling immediate surface-level identification of migration errors.
