import argparse
import logging

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Business contexts
    parser.add_argument(
        "--import-email",
        nargs="?",
        help="Downloads the email data from gmail and loads it up in postgres database",
        type=bool,
    )
    parser.add_argument(
        "--rule-engine",
        nargs="?",
        help="Allows to configure rules and do operations",
        type=bool,
    )
    # Other contexts
    parser.add_argument("--verbose", nargs="?", help="Provides verbose logs", type=bool)
    parser.parse_args()
