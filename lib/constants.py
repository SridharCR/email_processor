from datetime import datetime

FIELD_MAP = {
    "email_from": str,
    "subject": str,
    "received_date": datetime,
    "message": str,
}

PREDICATE_MAP = {
    "contains": str,
    "not equals": str,
    "equals": str,
    "less than": datetime,
}
