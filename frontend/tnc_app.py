from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.security.oauth2 import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import boto3
from google.oauth2 import id_token
from google.auth.transport import requests as g_requests
import os
from urllib.parse import urlparse
import uuid
import time
import json
import hashids

app = FastAPI()

# Assuming you have appropriate settings in the JSON file
settings = json.load(open("app_settings.json"))
ddb_client = boto3.client("dynamodb", "us-east-2")
s3_client = boto3.client("s3", "us-east-2")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Rest of your code

@app.get("/")
def main(request: Request, logged_in: bool = Depends(logged_in_user)):
    # Your main route code
    return templates.TemplateResponse(
        "main.html",
        {
            "request": request,
            "email": session.get("user_info")["user_email"],
            "admin": session.get("user_info")["admin"],
            "STAGE": STAGE,
        },
    )

@app.get("/landing")
def landing(request: Request, logged_in: bool = Depends(logged_in_user)):
    # Your landing route code
    return templates.TemplateResponse(
        "landing.html",
        {
            "request": request,
            "server_name": app.config["SERVER_NAME"],
            "oauth_id": settings["GOOGLE_OAUTH_ID"],
            "redirect_url": url_for("login"),
            "STAGE": STAGE,
        },
    )

@app.get("/login")
def login(request: Request, credential: str, logged_in: bool = Depends(logged_in_user)):
    # Your login route code
    # Consider refactoring to a separate function
    req = g_requests.Request()
    id_info = id_token.verify_oauth2_token(credential, req)
    user = get_or_create_user(id_info)
    session["logged_in"] = True
    session["user_info"] = user
    return {"OK": True}

# Rest of your routes

def logged_in_user(token: str = Depends(oauth2_scheme)):
    # Your user authentication logic using the token
    # Return True if logged in, False otherwise
    return True  # Example logic

# Rest of your functions

if __name__ == "__main__":
    app.run()
