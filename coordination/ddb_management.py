from fastapi import FastAPI
import boto3
import json

app = FastAPI()

client = boto3.client('dynamodb', region_name="us-east-2")
settings = json.loads(open("../app_settings.json", "r").read())

@app.get("/create_job_table")
def create_job_table():
    response = client.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'job_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'user_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'record_type',
                'AttributeType': 'S'
            },
            {
                "AttributeName": "step",
                'AttributeType': "N"
            },
            {
                "AttributeName": "user_email",
                "AttributeType": "S"
            }
        ],
        TableName=settings["JOB_TABLE"],
        KeySchema=[
            {
                "AttributeName": "job_id",
                'KeyType': "HASH"
            }
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                'IndexName': settings["USR_INDEX"],
                'KeySchema': [{
                    "AttributeName": "user_id",
                    "KeyType": "HASH"
                }],
                "Projection": {
                    "ProjectionType": "ALL"
                }
            },
            {
                'IndexName': settings["EMAIL_INDEX"],
                'KeySchema': [{
                    "AttributeName": "user_email",
                    "KeyType": "HASH"
                }],
                "Projection": {
                    "ProjectionType": "ALL"
                }
            },
            {
                'IndexName': settings["REC_TYPE_INDEX"],
                'KeySchema': [{
                    "AttributeName": "record_type",
                    "KeyType": "HASH"
                }],
                "Projection": {
                    "ProjectionType": "ALL"
                }
            },
            {
                'IndexName': settings["STEP_INDEX"],
                'KeySchema': [{
                    "AttributeName": "step",
                    "KeyType": "HASH"
                }],
                "Projection": {
                    "ProjectionType": "ALL"
                }
            }
        ]
    )
    return response

@app.get("/create_session_table")
def create_session_table():
    response = client.create_table(
        AttributeDefinitions=[{
            "AttributeName": "id",
            "AttributeType": "S"
        }],
        KeySchema=[{
            "AttributeName": 'id',
            "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        TableName=settings["SESSION_TABLE"]
    )
    return response

@app.get("/delete_job_table")
def delete_job_table():
    response = client.delete_table(TableName=settings["JOB_TABLE"])
    return response
