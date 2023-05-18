"""
Helper module to talk with gmail apis.
"""

import json
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from lib.log import logger

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


class GmailApi:
    """Gmail helper class"""

    def __init__(self):
        self.credentials = self.authenticate_gmail()

    def authenticate_gmail(self):
        """
        Authenticate gmail API with OAuth2.0 with provided client_id and client_secret

        :return: dict
        """
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        return creds

    def get_messages(self):
        """
        Fetch the list of message ids and their email data and metadata
        :return: list
        """
        try:
            logger.info("Starting to fetch the emails from Gmail")
            service = build("gmail", "v1", credentials=self.credentials)
            request = service.users().messages().list(userId="me")
            results = request.execute()
            message_ids = results.get("messages", [])
            if not message_ids:
                logger.info("No messages found.")
                return
            logger.info("Fetched the emails from Gmail")
            logger.info("Starting to fetch the email data and its metadata from Gmail")
            batch = service.new_batch_http_request()
            for msg_id in message_ids:
                batch.add(service.users().messages().get(userId="me", id=msg_id["id"]))
            batch.execute()
            result_values = list(batch._responses.values())
            result = [json.loads(each[1].decode()) for each in result_values]
            logger.info("Fetched the email data and its metadata from Gmail")
            return result

        except Exception as error:
            logger.error("Error occurred while getting messages from gmail: %s" % error)

    def do_actions(self, action_payload, desc):
        """
        Modify the labels of the email
        :param action_payload: dict
        :param desc: str
        :return: None
        """
        try:
            logger.debug("Entering do_actions()")
            service = build("gmail", "v1", credentials=self.credentials)
            request = (
                service.users().messages().batchModify(userId="me", body=action_payload)
            )
            results = request.execute()
            if results == "":
                logger.info("Email action - %s applied successfully" % desc)
            logger.debug("Exiting do_actions()")

        except Exception as ex:
            logger.error("Error occurred while moving the messages in gmail: %s" % ex)
