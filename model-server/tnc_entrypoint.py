import tnc_detector
import json, time, zipfile, os, shutil, uuid
import boto3
import argparse
import requests

import tensorflow as tf

from simple_scheduler.recurring import recurring_scheduler

ZIP_PATH = "./zips"
TASK_PATH = "./tasks"
OUT_PATH = "./output"

settings = json.load(open("app_settings.json"))
ddb_client = boto3.client("dynamodb", "us-east-2")
s3_client = boto3.client("s3", "us-east-2")


def reset_env():
	if os.path.isdir(ZIP_PATH):
		shutil.rmtree(ZIP_PATH)

	if os.path.isdir(TASK_PATH):
		shutil.rmtree(TASK_PATH)

	if os.path.isdir(OUT_PATH):
		shutil.rmtree(OUT_PATH)


def setup_env():
	if not os.path.isdir(ZIP_PATH):
		os.mkdir(ZIP_PATH)

	if not os.path.isdir(TASK_PATH):
		os.mkdir(TASK_PATH)

	if not os.path.isdir(OUT_PATH):
		os.mkdir(OUT_PATH)
	print("env successfully setup")


class detector_job_manager():

	# One class which manages downloading, detecting, and uploading.
	# largely just for the convenience of keeping the paths organized between the tasks.

	def __init__(self, model, ddb_result):
		print("Job Starting")
		print(ddb_result)
		self.model = model

		self.job_id = ddb_result["job_id"]  # the primary key for the job table
		self.location = ddb_result["upload_location"]["S"]
		# we only need the key to download from s3- so the filename
		self.fname = self.location.split("/")[-1]
		# the task id is the original upload name plus a hash
		self.task_id = self.fname.split(".")[0]
		# the task name is just the original upload name, no hash:
		# it's still present in the directory structure of the zipfile.
		self.task_name = self.task_id.split("--")[0]

		# where we will dl the raw zip to
		self.zip_loc = f"{ZIP_PATH}/{self.fname}"

		# where we will unzip it to
		self.unzip_loc = f"{TASK_PATH}/{self.task_id}-raw"

		self.task_loc = f"{TASK_PATH}/{self.task_id}"

		# where we will output results to
		self.output_loc = f"{OUT_PATH}/{self.task_id}"
		self.output_zip_name = f'{ZIP_PATH}/{self.task_id}-output'
		self.output_zip = self.output_zip_name + ".zip"
		self.output_fname = self.output_zip.split("/")[-1]

		self.error = False

	def download(self):
		report_run(f'Download: {self.job_id["S"]}')
		s3_client.download_file(settings["S3_BUCKET"], self.fname, self.zip_loc)

		f = open(self.zip_loc, "rb")
		z = zipfile.ZipFile(f)

		# sizelog = open("sizelog.txt", 'a')
		print(f'======== JOB {self.job_id} ========\n')
		print("zipsize: " + str(os.path.getsize(self.zip_loc)) + "\n")

		z.extractall(self.unzip_loc)
		f.close()

		print("LALALALA")
		ddb_resp = ddb_client.update_item(
			TableName=settings["JOB_TABLE"],
			Key={
				"job_id": self.job_id
			},
			UpdateExpression="SET step = :step_val",
			ExpressionAttributeValues={
				":step_val": {"N": "1"},

			}
		)

		os.mkdir(self.task_loc)
		bad_files = []
		num_f = 0

		for dpath, dname, fnames in os.walk(self.unzip_loc):
			for f in fnames:
				if (f[0] != "."):
					if (".jpg" in f or ".jpeg" in f or ".JPG" in f):
						num_f += 1
						os.rename(os.path.join(dpath, f), os.path.join(self.task_loc, f))
					else:
						bad_files.append(f)
						self.error = True

		if self.error:
			ddb_resp = ddb_client.update_item(
				TableName=settings["JOB_TABLE"],
				Key={
					"job_id": self.job_id
				},
				UpdateExpression="SET step = :step_val, error_msg=:error_msg",
				ExpressionAttributeValues={
					":step_val": {"N": "3"},
					":error_msg": {"S": "Bad Zipfile"}
				}

			)

	def do_detection_task(self):
		report_run(f'Model Start: {self.job_id["S"]}')
		tnc_detector.main(self.model, self.task_loc, OUT_PATH)

	def put_results(self):
		# zip the results
		zip_name = shutil.make_archive(self.output_zip_name, 'zip', self.output_loc)
		# upload to s3
		s3_resp = s3_client.put_object(
			Bucket=settings["S3_BUCKET"],
			Body=open(f'{self.output_zip}', 'rb'),
			Key=self.output_fname
		)

		# update the ddb table
		print("This must run, right?")
		ddb_resp = ddb_client.update_item(
			TableName=settings["JOB_TABLE"],
			Key={
				"job_id": self.job_id
			},
			UpdateExpression="SET step = :step_val, output_location=:output_val",
			ExpressionAttributeValues={
				":step_val": {"N": "2"},
				":output_val": {"S": self.output_fname}

			}

		)
		# print(s3_resp)
		# print(ddb_resp)
		print(f"{self.job_id}: SUCCESS \n")

	def run_job(self):
		report_run(f'Start: {self.job_id["S"]}')
		setup_env()
		self.download()
		if (not self.error):
			self.do_detection_task()
			self.put_results()
			reset_env()
			report_run(f'End: {self.job_id["S"]}')
		else:
			report_run(f'Error: {self.job_id["S"]}')


# Queries the dynamodb for jobs which are in step 0 (ie, just uploaded, this might change)
# Downloads them to the current filesystem
# and unzips them
def do_queued_jobs(model):
	queued_jobs = ddb_client.query(
		TableName=settings["JOB_TABLE"],
		IndexName=settings["STEP_INDEX"],
		Select="ALL_ATTRIBUTES",
		KeyConditionExpression="step = :step_val",
		ExpressionAttributeValues={":step_val": {"N": "0"}}
	)
	print(f'Found {len(queued_jobs["Items"])} jobs in ddb')
	report_run(f'Found {len(queued_jobs["Items"])} jobs in ddb')
	for job in queued_jobs['Items']:
		manager = detector_job_manager(model, ddb_result=job)
		manager.run_job()


def do_task(model="megadetector_v4_1_0.pb"):
	print("Task is starting- will query ddb")
	# Give the model by default here for scheduler's sake, while modularity isn't a concern.
	setup_env()
	do_queued_jobs(args.model)


parser = argparse.ArgumentParser(description="Nature Conservancy Image Detector")
parser.add_argument("model", help="The path to the Megadetector model to use for detection")


def report_run(message):
	item = {
		'record_type': {"S": "LOG"},
		'job_id': {"S": uuid.uuid1().hex},
		'user_id': {"S": "null"},
		'state': {"S": message},
		'step': {"N": "-1"},
		'timestamp': {"N": f'{time.time()}'},
	}
	ddb_client.put_item(
		TableName=settings["JOB_TABLE"],
		Item=item
	)


if __name__ == "__main__":
	args = parser.parse_args()
	report_run(f"container running: gpu_available={tf.config.experimental_list_devices()}")
	do_task(args.model)
