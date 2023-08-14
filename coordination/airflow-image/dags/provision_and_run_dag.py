from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import ECSOperator
from airflow.providers.amazon.aws.sensors.sqs import SQSSensor
from airflow.sensors.base import BaseSensorOperator


#Many of these should be set either in the general app settings
#or in the DAG configuration. This will do for now, however. 
instance_type = "p2.xlarge"
ecs_ami_id = "ami-0adc4758ea76442e2"
supported_gpu_ami = "ami-00e5ec752ce78b954"
min_count=1
max_count=1
IAM_profile = { 
#This allows the instance to be discoverable by ECS, and allows the model server access to aws services when run on it
	"Arn":"arn:aws:iam::091733705266:instance-profile/EC2InstanceRole"
}

cluster_arn = "arn:aws:ecs:us-east-2:091733705266:cluster/default"
task_def = "arn:aws:ecs:us-east-2:091733705266:task-definition/tnc-model-default-network:1"
gpu_task_def = "arn:aws:ecs:us-east-2:091733705266:task-definition/p2-gpu-compat:3"

sqs_queue_url = "https://us-east-2.queue.amazonaws.com/091733705266/tnc-queue"

InstanceTags = [{
	'ResourceType':"instance", #Required, but why???
	'Tags':[
		{"Key":"project",
		"Value": "tnc"},
		{"Key":"Name",
		"Value":f"airflow-provisioned-{instance_type}"}
		,{"Key":"ecs", #So we can grab it to DESTROY IT later
		"Value":"true"}
	]
}]

#settings = json.loads(open("../app_settings.json", "r").read())


#This is a sensor operator which checks the ecs cluster for an instance with our given id. 
# Just so we don't end up trying to place the task without the compute available. 
class ECSInstanceReadySensor(BaseSensorOperator):
	
	def __init__(self, getInstance_Task:str, **kwargs):
		super().__init__(**kwargs)
		self.getInstance_Task = getInstance_Task

	def poke(self,context: 'Context'):
		import boto3
		ec2_id = context['task_instance'].xcom_pull(task_ids=self.getInstance_Task)
		ecs = boto3.client("ecs", region_name="us-east-2")
		instances = ecs.list_container_instances(
			cluster="default", 
			
			filter=f"ec2InstanceId=={ec2_id} and agentConnected==true"
			)
		print(instances)
		return len(instances["containerInstanceArns"]) > 0


#Use this instead of the sqs operator
#Just uses the same ddb query that the model server does to start.
#No extra infrastructure needed, and no risks of weird edge-cases like the queue might have
class TncDDBJobSensor(BaseSensorOperator):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

	def poke(self, context:'Context'):
		import boto3
		ddb_client = boto3.client('dynamodb', region_name="us-east-2")
		queued_jobs = ddb_client.query(
			TableName = "tnc-job-table-test",
			IndexName = "step-index",
			Select = "ALL_ATTRIBUTES",
			KeyConditionExpression = "step = :step_val",
			ExpressionAttributeValues = {":step_val": {"N": "0"}}
		)
		return len(queued_jobs["Items"]) > 0

#Try to run the dag every minute
#But if it's already running,  then just stop

with DAG(
	"tnc-run-model",
	start_date=datetime(2022, 1, 28),
	default_view="tree",
    schedule_interval="* * * * *",
    concurrency=1,
    catchup=False,
    max_active_runs=1) as dag:
	
	
	#ecs status task
	def setup_instance():
		import boto3

		ec2 = boto3.resource('ec2',region_name="us-east-2")
		ec2_client = boto3.client("ec2", region_name="us-east-2")

		filters = [
        	{
        	    'Name': 'instance-state-name', 
            	'Values': ['running','stopped','stopping'] 
            	#"stopping" too in case the dag is triggered while the instance is still cooling down from the last run
        	},{
        		'Name':'tag-key',
        		'Values':['ecs']
        	},{
        		"Name":"instance-type",
        		"Values":[instance_type]
        	}	
    	]
    	#Get all the instances which meet the above criteria
		instance_list = ec2.instances.filter(Filters=filters)
		instance_list = [i for i in instance_list]
		#There should be no more than one... and if it exists, we're already golden
		if(len(instance_list) > 0):
			print('instance found')
			instance = instance_list[0]
			#we do have to start it if it's stopped though. (it should probably be stopped)
			if instance.state["Name"] in ["stopped","stopping"]:
				print("instance starting")
				instance.start()
			#The instance id gets pushed to xcom so it can be used later on
			return instance.id

		else:
			print("instance not found, provisioning")
			#Provision a new instance meeting the task requirements if none exists to be started. 
			resp = ec2_client.run_instances(
				ImageId=supported_gpu_ami,
				InstanceType=instance_type,
				MinCount=min_count,
				MaxCount=max_count,
				IamInstanceProfile=IAM_profile,
				TagSpecifications=InstanceTags
				)
			instance_id = resp["Instances"][0]["InstanceId"]
			#the instance id gets pushed to xcom so it can be used later on, same as above
			return instance_id
	

	
	#We want to stop the instance we spun up when we're done with it- just like this.
	def stop_instance(getInstance_Task="setup-instance", **context):
		import boto3
		ec2_client = boto3.client("ec2", region_name="us-east-2")
		instance_id = context['ti'].xcom_pull(task_ids=getInstance_Task)
		resp = ec2_client.stop_instances(
			InstanceIds=[instance_id]
		)
		#this just hangs executition until we have confirmed that the instance is fully stopped
		#that way, we don't run into a case where we're trying to run the task again
		#immediately after, while the instance is still in the 'stopping' state (from which it can't be started)
		waiter = ec2_client.get_waiter('instance_stopped')
		waiter.wait(InstanceIds=[instance_id])
		return


	DDB_SENSOR = TncDDBJobSensor(task_id="ddb-sensor")


	SETUP_INSTANCE = PythonOperator(
		task_id="setup-instance",
		python_callable=setup_instance
	) 

	ECS_READY = ECSInstanceReadySensor(
		task_id="ecs-ready-sensor", 
		getInstance_Task="setup-instance"
	)

	#aws provider package has us covered here. 
	#it even remains 'running' as long as the task it starts does, so we can
	#use this task state as a proxy for the model state 
	RUN_MODEL = ECSOperator(
		region_name="us-east-2",
	    cluster=cluster_arn,
    	task_definition=gpu_task_def,
    	launch_type="EC2",
    	overrides={},
    	task_id="airflow-provisioned-tnc-model",
  		tags={
        	"Project": "tnc",
    	})

	STOP_INSTANCE = PythonOperator(task_id="stop-instance",
		python_callable=stop_instance
	)

	DDB_SENSOR >> SETUP_INSTANCE >> ECS_READY >> RUN_MODEL >> STOP_INSTANCE


