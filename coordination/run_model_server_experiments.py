from fastapi import FastAPI
import boto3
import json

app = FastAPI()

settings = json.loads(open("../app_settings.json", "r").read())

ecs_client = boto3.client("ecs", region_name=settings["region"])


@app.get("/run_task")
def run_task():
	cluster_arn = "arn:aws:ecs:us-east-2:091733705266:cluster/default"
	task_def = "arn:aws:ecs:us-east-2:091733705266:task-definition/tnc-model-default-network:1"

	resp = ecs_client.run_task(
		cluster=cluster_arn,
		launchType="EC2",
		taskDefinition=task_def
	)

	return resp
