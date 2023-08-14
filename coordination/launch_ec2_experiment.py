from fastapi import FastAPI
import boto3
import json

app = FastAPI()

settings = json.loads(open("../app_settings.json", "r").read())

ecs_client = boto3.client("ecs", region_name=settings["region"])
ec2_client = boto3.client("ec2", region_name=settings["region"])


@app.get("/launch_instance")
def launch_instance():
	ecs_ami_id = "ami-0adc4758ea76442e2"
	ecs_gpu_ami_id = "ami-0ddcae9b03243372b"
	supported_gpu_ami = "ami-00e5ec752ce78b954"
	instance_type = "p2.xlarge"
	min_count = 1
	max_count = 1
	IAM_profile = {
		"Arn": "arn:aws:iam::091733705266:instance-profile/EC2InstanceRole"
	}
	Tags = [{
		'ResourceType': "instance",
		'Tags': [
			{"Key": "project",
			 "Value": "tnc"},
			{"Key": "Name",
			 "Value": f"airflow-provisioned-{instance_type}"},
			{"Key": "ecs",
			 "Value": "true"}
		]
	}]

	resp = ec2_client.run_instances(
		ImageId=ecs_ami_id,
		InstanceType=instance_type,
		MinCount=min_count,
		MaxCount=max_count,
		IamInstanceProfile=IAM_profile,
		TagSpecifications=Tags
	)

	return resp
