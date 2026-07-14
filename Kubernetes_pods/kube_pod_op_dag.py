from datetime import timedelta
import airflow
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

default_args = {
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dvt_dag_kubernetes_pod_operator",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    run_dvt = KubernetesPodOperator(
        # The ID specified for the task.
        task_id="dvt-validation",
        name="dvt-validation",
        cmds=["bash", "-cx"],
        # Performs a simple column validation on public data
        arguments=[
            "source $HOME/.venv/dvt/bin/activate && data-validation connections add --connection-name bq-connect BigQuery --project-id {{PROJECT_ID}} && data-validation validate column -sc bq-connect -tc bq-connect -tbls bigquery-public-data.new_york_citibike.citibike_trips -rh {{PROJECT_ID}}.pso_data_validator.results "
        ],
        # The namespace to run within Kubernetes. In Composer 2 environments
        # after December 2022, the default namespace is
        # `composer-user-workloads`.
        namespace="composer-user-workloads",
        # DVT image built from README instructions
        image="gcr.io/{{PROJECT_ID}}/data-validation",
        # default to '~/.kube/config'. The config_file is templated.
        config_file="/home/airflow/composer_kube_config",
        # Identifier of connection that should be used
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        env_vars={
            # Uncomment this env variable to reference connections stored in a GCS bucket
            # "PSO_DV_CONN_HOME": "gs://{{BUCKET_NAME}}/",
            "GOOGLE_CLOUD_PROJECT": "{{PROJECT_ID}}",
        },
    )
