from fastapi import FastAPI, HTTPException
import boto3
import json

app = FastAPI()

# Load the settings from the app_settings.json file
settings = json.loads(open("../app_settings.json", "r").read())

# Create an AWS SQS client
sqs_client = boto3.client("sqs")

# Define the FastAPI endpoint to create the SQS queue
@app.post("/create_sqs_queue")
def create_sqs_queue():
    try:
        resp = sqs_client.create_queue(QueueName=settings["TNC_QUEUE"])
        return {"message": "SQS queue created successfully", "queue_url": resp["QueueUrl"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
