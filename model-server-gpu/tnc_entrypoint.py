import tnc_detector
import json, time, zipfile, os, shutil, uuid
import boto3
import argparse
import requests
import humanfriendly
from simple_scheduler.recurring import recurring_scheduler
from flask import render_template
import yagmail

ZIP_PATH = "./zips"
TASK_PATH = "./tasks"
OUT_PATH = "./output"

settings = json.load(open("app_settings.json"))
ddb_client = boto3.client("dynamodb", "us-east-2")
s3_client = boto3.client("s3", "us-east-2")

def flatten_ddb(item):
    return {k:i[list(i)[0]] for k,i in item.items()}

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

	#One class which manages downloading, detecting, and uploading.
	#largely just for the convenience of keeping the paths organized between the tasks.

	def __init__(self, model, ddb_result):
		print("Job Starting")
		self.start_time = time.time()
		self.model = model

		self.user_email = ddb_result["user_email"]["S"]
		self.job_id = ddb_result["job_id"] #the primary key for the job table
		self.location = ddb_result["upload_location"]["S"]
		#we only need the key to download from s3- so the filename 
		self.fname = self.location.split("/")[-1] 
		#the task id is the original upload name plus a hash
		self.task_id = self.fname.split(".")[0]
		#the task name is just the original upload name, no hash:
		#it's still present in the directory structure of the zipfile.
		self.task_name = self.task_id.split("--")[0]
		
		#where we will dl the raw zip to
		self.zip_loc = f"{ZIP_PATH}/{self.fname}"
		
		#where we will unzip it to
		self.unzip_loc = f"{TASK_PATH}/{self.task_id}-raw"
		

		self.task_loc = f"{TASK_PATH}/{self.task_id}"
		
		#where we will output results to
		self.output_loc = f"{OUT_PATH}/{self.task_id}"
		self.output_zip_name = f'{ZIP_PATH}/{self.task_id}-output'
		self.output_zip = self.output_zip_name+".zip"
		self.output_fname = self.output_zip.split("/")[-1]

		self.error = False

	def download(self):
		s3_client.download_file(settings["S3_BUCKET"], self.fname, self.zip_loc)

		f = open(self.zip_loc, "rb")
		z = zipfile.ZipFile(f)

		#sizelog = open("sizelog.txt", 'a')
		print(f'======== JOB {self.job_id} ========\n')
		print("zipsize: "+str(os.path.getsize(self.zip_loc)) + "\n")

		z.extractall(self.unzip_loc)
		f.close()

		
		
		os.mkdir(self.task_loc)
		bad_files = []
		self.num_f = 0

		for dpath, dname, fnames in os.walk(self.unzip_loc):
			for f in fnames:
				if(f[0] != "."):
					if(".jpg" in f or ".jpeg" in f or ".JPG" in f):
						self.num_f += 1
						os.rename(os.path.join(dpath, f), os.path.join(self.task_loc, f))
					else:
						bad_files.append(f)
						self.error = True

		duration_estimate = self.get_duration_estimate()
		ddb_resp = ddb_client.update_item(
			TableName = settings["JOB_TABLE"],
			Key = {
				"job_id":self.job_id
			},
			UpdateExpression= "SET step = :step_val, num_files = :num_f, duration_estimate = :est, start_time = :start",
			ExpressionAttributeValues = {
				":step_val":{"N":"1"}, 
				":num_f":{"N":str(self.num_f)},
				":est":{"N":str(duration_estimate)},
				":start":{"N":str(self.start_time)}
			}
		)


		if self.error:
			ddb_resp = ddb_client.update_item(
			TableName = settings["JOB_TABLE"],
			Key = {
				"job_id":self.job_id
			},
			UpdateExpression= "SET step = :step_val, error_msg=:error_msg",
			ExpressionAttributeValues = {
				":step_val":{"N":"3"},
				":error_msg":{"S": "Bad Zipfile"}
			}

		)
		

	def do_detection_task(self):
		tnc_detector.main(self.model, self.task_loc, OUT_PATH)
		self.elapsed = time.time() - self.start_time

	def put_results(self):
		#zip the results
		zip_name = shutil.make_archive(self.output_zip_name,'zip', self.output_loc)
		#upload to s3
		s3_resp = s3_client.put_object(
			Bucket=settings["S3_BUCKET"],
			Body=open(f'{self.output_zip}', 'rb'),
			Key=self.output_fname
		)

		#update the ddb table

		ddb_resp = ddb_client.update_item(
			TableName = settings["JOB_TABLE"],
			Key = {
				"job_id":self.job_id
			},
			UpdateExpression= "SET step = :step_val, output_location=:output_val, run_duration=:duration",
			ExpressionAttributeValues = {
				":step_val":{"N":"2"},
				":output_val":{"S":self.output_fname},
				":duration":{"N":str(self.elapsed)}
			}

		)
		#print(s3_resp)
		#print(ddb_resp)
		print(f"{self.job_id}: SUCCESS \n")

	def get_duration_estimate(self):
		finished_jobs = ddb_client.query(
			TableName = settings["JOB_TABLE"],
			IndexName = settings["STEP_INDEX"],
			Select = "ALL_ATTRIBUTES",
			KeyConditionExpression = "step = :step_val",
			ExpressionAttributeValues = {":step_val": {"N": "2"}}
		)
		#we want the average time per image- so just sum all durations and all files...
		jobs = [flatten_ddb(i) for i in finished_jobs["Items"]]
		if(len(jobs) > 0):
			total_duration = sum(float(j["run_duration"]) for j in jobs)
			total_files = sum(float(j["num_files"]) for j in jobs)
			per_file_duration = total_duration / total_files
		else:
			#default_Estimate
			per_file_duration = 2

		return per_file_duration * int(self.num_f)
		

	def run_job(self):
		setup_env()
		self.download()
		if(not self.error):
			self.do_detection_task()
			self.put_results()
			reset_env()
			#report_run(f'End {self.job_id["S"]} in {humanfriendly.format_timespan(self.elapsed)}')
			self.notify()
		else:
			report_run(f'Error: {self.job_id["S"]}')

	def notify(self):
		send_mail(self.user_email, 
			"Your Foxbounder job has completed", 
			f"Your Foxbounder job has completed!")


#Queries the dynamodb for jobs which are in step 0 (ie, just uploaded, this might change)
#Downloads them to the current filesystem
#and unzips them
def do_queued_jobs(model):
	queued_jobs = ddb_client.query(
			TableName = settings["JOB_TABLE"],
			IndexName = settings["STEP_INDEX"],
			Select = "ALL_ATTRIBUTES",
			KeyConditionExpression = "step = :step_val",
			ExpressionAttributeValues = {":step_val": {"N": "0"}}
		)
	print(f'Found {len(queued_jobs["Items"])} jobs in ddb')
	if(len(queued_jobs["Items"]) == 0):
		return
	else:
		for job in queued_jobs['Items']:
			manager = detector_job_manager(model, ddb_result=job)
			manager.run_job()
		do_queued_jobs(model)




def do_task(model = "megadetector_v4_1_0.pb"):
	print("Task is starting- will query ddb") 
	#Give the model by default here for scheduler's sake, while modularity isn't a concern.
	setup_env()
	do_queued_jobs(args.model)
	


parser = argparse.ArgumentParser(description="Nature Conservancy Image Detector")
parser.add_argument("model", help="The path to the Megadetector model to use for detection")

def report_run(message):
	item = {
		'record_type':{"S":"LOG"},
		'job_id':{"S": uuid.uuid1().hex},
		'user_id':{"S":"null"},
		'state':{"S":message},
		'step':{"N":"-1"},
		'timestamp':{"N":f'{time.time()}'},
	}
	ddb_client.put_item(
		TableName = settings["JOB_TABLE"],
		Item = item
	)

def send_ses(email, subject, message):
	mailer.send_email(
	    Source=settings["OUTBOUND_ADDRESS"],
	    Destination={
	        'ToAddresses': [
	            email,
	        ],
	    },
	    Message={
	        'Subject': {
	            'Data': subject
	        },
	        'Body': {
	            'Html': {
	                'Data': render_template("mail_template.html",
	                	CONTENT=message)

	            },
	        }
	    },
	    Tags=[
	        {
	            'Name': 'project',
	            'Value': 'tnc'
	        },
	    ],
	)


#This is a backup while I still have to sort out some permissions nonsense
def send_yag(email, subject,message):
	pwd = "mreatidbwderdaag"
	with yagmail.SMTP(settings["OUTBOUND_ADDRESS"], pwd) as yag:
	    yag.send(email, subject, message)

def send_mail(email, subject, message):
	try:
		send_yag(email, subject, message)
	except:
		print("Mailing Error, gmail is borked")

if __name__ == "__main__":
	args = parser.parse_args()
	#report_run(f"container running")
	do_task(args.model)
    