import datetime
import os
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

#This adapts 'run_model_server_experiments.py' into a DAG, after the example in
#https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/example_dags/example_ecs_fargate.py

print(os.getcwd())

cluster_arn = "arn:aws:ecs:us-east-2:091733705266:cluster/default"
task_def = "arn:aws:ecs:us-east-2:091733705266:task-definition/tnc-model-default-network:1"
gpu_task_def = "arn:aws:ecs:us-east-2:091733705266:task-definition/p2-gpu-compat:3"

dag = DAG(
    dag_id="invoke_tnc_model_dag",
    default_view="graph",
    schedule_interval=None,
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
    tags=["example"],
)


run_model = ECSOperator(
	region_name="us-east-2",
    dag=dag,
    cluster=cluster_arn,
    task_definition=gpu_task_def,
    launch_type="EC2",
    overrides={},
    task_id="airflow-provisioned-tnc-model",
    tags={
        "Project": "tnc",
    },
   
)