import argparse
import logging

from src.rule_processor.dao.email_db import email_db_cleanup
from src.rule_processor.middlewares.email_loader import EmailLoader
from src.rule_processor.middlewares.rule_engine import option_builder

logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)


def import_email():
    EmailLoader().process()
    return "True"


def rule_engine():
    option_builder()
    return "True"


def db_cleanup():
    email_db_cleanup()
    return "True"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-cleanup",
        nargs="?",
        help="Downloads the email data from gmail and loads it up in postgres database",
        type=bool,
    )
    parser.add_argument(
        "--import-email",
        nargs="?",
        help="Downloads the email data from gmail and loads it up in postgres database",
        type=bool,
    )
    parser.add_argument(
        "--rule-engine",
        nargs="?",
        help="Allows to configure conditions and do operations",
        type=bool,
    )
    # Other contexts
    parser.add_argument("--verbose", nargs="?", help="Provides verbose logs", type=bool)
    args = parser.parse_args()

    if args.db_cleanup:
        email_db_cleanup()
    if args.import_email:
        import_email()
    if args.rule_engine:
        rule_engine()
