from __future__ import print_function

import datetime
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy.orm import Session

from lib.db import postgresql_engine
from lib.log import logger
from src.rule_processor.email_db import EmailMetadata, EmailBody

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def authenticate_gmail():
    """
    Authenticate gmail API with OAuth2.0 with provided client_id and client_secret

    :return: dict
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def get_messages():
    try:
        creds = authenticate_gmail()
        # Call the Gmail API
        service = build("gmail", "v1", credentials=creds)
        request = service.users().messages().list(userId="me")
        results = request.execute()
        messages = results.get("messages", [])
        # messages = messages[:1]
        if not messages:
            print("No messages found.")
            return
        print("Messages:")
        sri = None
        for msg in messages:
            txt = service.users().messages().get(userId="me", id=msg["id"]).execute()
            sri = txt
            transformer(sri)

    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f"An error occurred: {error}")


def transformer(gmail_data):
    logger.info("Email data transformation initiated")

    session = None

    db_gmail_key_mapper = {
        "id": {
            "col_name": "id",
            "formatter": lambda x: x,
        },
        "threadId": {
            "col_name": "thread_id",
            "formatter": lambda x: x,
        },
        "historyId": {
            "col_name": "history_id",
            "formatter": lambda x: x,
        },
        "sizeEstimate": {
            "col_name": "size_estimate",
            "formatter": lambda x: x,
        },
        "internalDate": {
            "col_name": "received_date",
            "formatter": lambda x: datetime.datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S %z'),
        },
        "headers": {
            "From": {"col_name": "email_from", "formatter": lambda x: x},
            "To": {"col_name": "email_to", "formatter": lambda x: x},
            "Subject": {"col_name": "subject", "formatter": lambda x: x},
        },
    }

    try:
        session = Session(postgresql_engine)

        email_metadata = { }

        for gmail_key, db_asso_dict in db_gmail_key_mapper.items():
            if gmail_data.get(gmail_key):
                email_metadata[db_asso_dict["col_name"]] = db_asso_dict["formatter"](gmail_data[gmail_key])

        email_metadata_headers = {}

        for each_header in gmail_data["payload"].get("headers", []):
            if db_gmail_key_mapper["headers"].get(each_header["name"]):
                associated_dict = db_gmail_key_mapper["headers"][each_header["name"]]
                formatted_value = each_header["value"]
                print(formatted_value)
                if associated_dict.get("formatter", None):
                    formatted_value = associated_dict["formatter"](each_header["value"])
                email_metadata_headers[
                    db_gmail_key_mapper["headers"][each_header["name"]]["col_name"]
                ] = formatted_value

        email_metadata.update(email_metadata_headers)

        email_bodies = []
        for each_part in gmail_data["payload"].get("parts", []):
            if each_part["body"].get("data"):
                email_body = {
                    "id": gmail_data["id"],
                    "part_id": each_part["partId"],
                    "size": each_part["body"]["size"],
                    "data": each_part["body"]["data"],
                }
                email_body_object = EmailBody(**email_body)
                email_bodies.append(email_body_object)

        email_metadata["email_body"] = email_bodies
        email_metadata_object = EmailMetadata(**email_metadata)
        session.add(email_metadata_object)

    except Exception as ex:
        logger.exception(f"Exception occurred while loading the mail data to postgres - {ex}")
        session.rollback()
        session.close()
        raise
    else:
        logger.info("Successfully loaded the mail data to postgres")
        session.commit()
        session.close()


if __name__ == "__main__":
    get_messages()
