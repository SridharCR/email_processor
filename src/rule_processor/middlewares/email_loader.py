"""
This module takes care of loading the emails from gmail and transforms accordingly.
"""

import base64
import datetime

from sqlalchemy.orm import Session

from lib.db import postgresql_engine
from lib.log import logger
from src.rule_processor.dao.email_db import EmailMetadata, EmailBody
from src.rule_processor.middlewares.gmail_apis import GmailApi


class EmailLoader:
    """
    Class helps to pull the email data from gmail and applies transformation and loads them to
    the database
    """

    def __init__(self):
        pass

    def process(self):
        """
        Initiates the core process of pulling email data and loading them in database
        :return:
        """
        gmail_api_obj = GmailApi()
        gmail_data = gmail_api_obj.get_messages()
        logger.info("Email data transformation initiated")
        for each_gmail_data in gmail_data:
            logger.info(
                "Transforming and loading this email id %s" % each_gmail_data["id"]
            )
            self.transformer(each_gmail_data)
        logger.info("Successfully loaded the mail data to postgres")

    def transformer(self, gmail_data):
        """
        Applies necessary transformation to the pulled gmail data and stores in the database
        :param gmail_data: list
        :return: None
        """
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
                "formatter": lambda x: datetime.datetime.utcfromtimestamp(
                    int(x) / 1000
                ).strftime("%Y-%m-%d %H:%M:%S %z"),
            },
            "headers": {
                "From": {"col_name": "email_from", "formatter": lambda x: x},
                "To": {"col_name": "email_to", "formatter": lambda x: x},
                "Subject": {"col_name": "subject", "formatter": lambda x: x},
            },
        }

        try:
            session = Session(postgresql_engine)

            email_metadata = {}

            for gmail_key, db_asso_dict in db_gmail_key_mapper.items():
                if gmail_data.get(gmail_key):
                    email_metadata[db_asso_dict["col_name"]] = db_asso_dict[
                        "formatter"
                    ](gmail_data[gmail_key])

            email_metadata_headers = {}

            for each_header in gmail_data["payload"].get("headers", []):
                if db_gmail_key_mapper["headers"].get(each_header["name"]):
                    associated_dict = db_gmail_key_mapper["headers"][
                        each_header["name"]
                    ]
                    formatted_value = each_header["value"]
                    if associated_dict.get("formatter", None):
                        formatted_value = associated_dict["formatter"](
                            each_header["value"]
                        )
                    email_metadata_headers[
                        db_gmail_key_mapper["headers"][each_header["name"]]["col_name"]
                    ] = formatted_value

            email_metadata.update(email_metadata_headers)

            email_metadata["email_body"] = []
            for each_part in gmail_data["payload"].get("parts", []):
                if each_part["body"].get("data"):
                    email_body = {
                        "id": gmail_data["id"],
                        "part_id": each_part["partId"],
                        "size": each_part["body"]["size"],
                        "data": base64.urlsafe_b64decode(each_part["body"]["data"]),
                    }
                    email_body_object = EmailBody(**email_body)
                    email_metadata["email_body"].append(email_body_object)

            email_metadata_object = EmailMetadata(**email_metadata)
            session.add(email_metadata_object)

        except Exception as ex:
            logger.exception(
                f"Exception occurred while loading the mail data to postgres - {ex}"
            )
            session.rollback()
            session.close()
        else:
            session.commit()
            session.close()
