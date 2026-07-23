from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Production Infrastructure Parameters
COMPOSER_BUCKET = os.environ.get("GCS_BUCKET", "your-composer-bucket").replace("gs://", "")
CONFIG_PREFIX = "dvt_configs/"
DVT_IMAGE = "us-central1-docker.pkg.dev/your-gcp-project-id/dvt-docker-repo/data-validation:v1" # ⚠️ MODIFY WITH YOUR STEP 3 PATH

default_args = {
    'owner': 'data-migration-platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'enterprise_dvt_orchestration_framework',
    default_args=default_args,
    description='Automated scaling validation engine parsing GCS configurations dynamically',
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Scan Cloud Storage for staged validation config files
    gcs_hook = GCSHook()
    all_blobs = gcs_hook.list(bucket_name=COMPOSER_BUCKET, prefix=CONFIG_PREFIX)
    yaml_configs = [blob for blob in all_blobs if blob.endswith(('.yaml', '.yml'))]

    # 2. Loop through discovered tables to generate parallel execution paths
    for config_blob in yaml_configs:
        file_name = os.path.basename(config_blob)
        sanitized_task_id = file_name.replace("_", "-").replace(".", "-").lower()

        # 3. Instantiate isolated GKE tracking containers via KubernetesPodOperator
        execute_dvt_pod = KubernetesPodOperator(
            task_id=f"run-{sanitized_task_id}",
            name=f"dvt-pod-{sanitized_task_id}",
            namespace="default",
            image=DVT_IMAGE,
            cmds=["run-validation", "--config-file", f"/workspace/{CONFIG_PREFIX}{file_name}"],
            
            # Optional: Concurrency throttling guard to protect Teradata spool boundaries
            # pool="teradata_dvt_pool", 
            
            # Mount the native Cloud Composer shared GCS FUSE file systems inside the pod space
            volumes=[
                k8s.V1Volume(
                    name="gcs-fuse-volume",
                    host_path=k8s.V1HostPathVolumeSource(path="/home/airflow/gcs")
                )
            ],
            volume_mounts=[
                k8s.V1VolumeMount(
                    name="gcs-fuse-volume",
                    mount_path="/workspace",
                    read_only=True
                )
            ],
            
            # Hardware resource allocation parameters
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "1Gi"},
                limits={"cpu": "2", "memory": "4Gi"}
            ),
            is_delete_operator_pod=True,
            get_logs=True
        )
