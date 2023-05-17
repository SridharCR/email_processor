from datetime import datetime

FIELD_MAP = {
    "email_from": str,
    "subject": str,
    "received_date": datetime,
    "data": str,
}

PREDICATE_MAP = {
    "contains": str,
    "not_equals": str,
    "equals": str,
    "less_than": datetime,
    "more_than": datetime,
}
