"""
Rule engine module to handle the conditions, predicates, and actions
"""

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from lib.constants import FIELD_MAP, PREDICATE_MAP
from lib.db import postgresql_engine
from lib.log import logger
from src.rule_processor.dao.email_db import EmailMetadata, EmailBody
from src.rule_processor.middlewares.gmail_apis import GmailApi


class Field:
    """
    Field class for condition
    """

    def __init__(self, field):
        self.field = field
        self.datatype = FIELD_MAP.get(field)
        self.valid_flag = self.is_valid()

    def is_valid(self):
        """
        Checks whether the field is valid or not.
        :return: bool
        """
        if not FIELD_MAP.get(self.field):
            logger.warning(
                "%s is not configured yet, kindly try with different field" % self.field
            )
            return False
        return True


class Predicate:
    """
    Predicate class for condition
    """

    def __init__(self, predicate):
        self.predicate = predicate
        self.datatype = PREDICATE_MAP.get(predicate)
        self.method = getattr(self, predicate) if hasattr(self, predicate) else None
        self.valid_flag = self.is_valid()

    def is_valid(self):
        """
        Checks whether the predicate is valid or not.
        :return: bool
        """
        if not PREDICATE_MAP.get(self.predicate):
            logger.warning(
                "%s is not configured yet, kindly try with different predicate"
                % self.predicate
            )
            return False
        return True

    def contains(self, key, value, **kwargs):
        """Creates a where clause for ilike"""
        return key.ilike(f"%{value}%")

    def does_not_contains(self, key, value, **kwargs):
        """Creates a where clause for not ilike"""
        return not key.ilike(f"%{value}%")

    def equals(self, key, value, **kwargs):
        """Creates a where clause for equals"""
        return key == value

    def does_not_equals(self, key, value, **kwargs):
        """Creates a where clause for not equals"""
        return key != value

    def less_than(self, key, value, **kwargs):
        """Creates a where clause for less than"""
        args = {kwargs["time_entity"]: int(value)}
        bound_range = datetime.now().today() - timedelta(**args)
        return key > bound_range

    def more_than(self, key, value, **kwargs):
        """Creates a where clause for more than"""
        args = {kwargs["time_entity"]: int(value)}
        bound_range = datetime.now().today() - timedelta(**args)
        return key < bound_range


class Condition:
    """
    Condition class to group the field, predicate and value
    """

    def __init__(self, field, predicate, value):
        self.field_obj = field
        self.predicate_obj = predicate
        self.value = value
        self.time_entity = None
        self.validate()
        self.result = []

    def validate(self):
        """
        Validates the inputs based on the datatypes
        :return: None
        """
        if self.field_obj.datatype == datetime:
            val = self.value.split(" ")
            self.value = val[0]
            self.time_entity = val[1]

            if self.time_entity not in [
                "days",
                "seconds",
                "minutes",
                "hours",
                "weeks",
                "months",
            ]:
                raise ValueError("Invalid time config")

            if self.time_entity == "months":
                self.value = self.value * 30
                self.time_entity = "days"

    def generate_where_clause(self):
        """
        Dynamically generates the where clause based on the field, predicate and value
        :return: sqlalchemy where
        """
        try:
            key = EmailMetadata.getattr(self.field_obj.field) or EmailBody.getattr(
                self.field_obj.field
            )
            where_statement = self.predicate_obj.method(
                key, self.value, time_entity=self.time_entity
            )
            return where_statement

        except Exception as ex:
            logger.exception("Error occurred while building predicate where clause")
            raise ex


class Action:
    """Available actions"""

    def __init__(self, move_to):
        self.move_to = move_to
        self.payload_move_to_map = {
            "read": {"removeLabelIds": ["UNREAD"]},
            "unread": {"addLabelIds": ["UNREAD"]},
        }
        self.payload = None
        if self.payload_move_to_map.get(move_to) is None:
            self.payload = {"removeLabelIds": [move_to.upper()]}
        else:
            self.payload = self.payload_move_to_map[move_to]


class ConditionActionGroup:
    """
    Condition class to handle the conditions and process the emails
    """

    def __init__(self, conditions, rule_group_predicate, actions):
        self.conditions = conditions
        self.rule_group_predicate = rule_group_predicate
        self.actions = actions
        self.result = []

    def process_emails(self):
        """
        Core method to initiate the processing
        :return: None
        """
        self.__filter_data()
        self.__apply_action()

    def __filter_data(self):
        """
        Filters the data based on the condition provided
        :return: list
        """
        session = None
        try:
            session = Session(postgresql_engine)
            cumulative_where_clause = None
            for each_rule in self.conditions:
                if cumulative_where_clause is not None:
                    if self.rule_group_predicate == "all":
                        cumulative_where_clause = cumulative_where_clause & (
                            each_rule.generate_where_clause()
                        )
                    elif self.rule_group_predicate == "any":
                        cumulative_where_clause = cumulative_where_clause | (
                            each_rule.generate_where_clause()
                        )
                else:
                    cumulative_where_clause = each_rule.generate_where_clause()
            statement = (
                select(EmailMetadata).join(EmailBody).filter(cumulative_where_clause)
            )
            rows = session.execute(statement).all()
            for each in rows:
                self.result.append(each[0].id)
            self.result = list(set(self.result))
        except Exception:
            session.rollback()
            session.close()
            logger.exception("Error occurred while preparing the filter query")
            raise

    def __apply_action(self):
        """
        Applies the specified action to the gmail
        :return: None
        """
        if self.result:
            logger.info("%s Matching emails found" % len(self.result))
            for each_action in self.actions:
                payload = each_action.payload
                payload["ids"] = self.result
                gmail_obj = GmailApi()
                gmail_obj.do_actions(each_action.payload, each_action.move_to)
        else:
            logger.info("No matching emails found, hence skipping")


def option_builder():
    """
    Interactive shell to build the conditions with rules that includes the field, predicate and
     value, then with the move actions with overall conditional predicate
    :return: None
    """
    print("Condition building: Enter your conditions")
    rule_list = []
    action_list = []
    new_rule_flag = True
    new_action_flag = True
    rule_predicate = input("If (any/all) of the conditions are met:")
    while new_rule_flag:
        field_valid_flag = False
        field = None
        while not field_valid_flag:
            field = Field(
                input(
                    f"Select a field from the given list ({','.join(list(FIELD_MAP.keys()))}): "
                )
            )
            field_valid_flag = field.valid_flag

        predicate_valid_flag = False
        predicate = None
        while not predicate_valid_flag:
            predicate = Predicate(
                input(
                    f"Select a predicate from the given list \
                    ({','.join(list(PREDICATE_MAP.keys()))}): "
                )
            )
            predicate_valid_flag = predicate.valid_flag

        value = input("Enter the value: ")

        condition = Condition(field, predicate, value)
        rule_list.append(condition)
        new_rule_flag = input("Do you want to add another condition? (Yes/No):")
        new_rule_flag = new_rule_flag == "Yes"

    while new_action_flag:
        action = Action(input("Select an action(read/unread/inbox): "))
        action_list.append(action)
        new_action_flag = input("Do you want to add another action? (Yes/No):")
        new_action_flag = new_action_flag == "Yes"

    cond_object = ConditionActionGroup(rule_list, rule_predicate, action_list)
    cond_object.process_emails()


if __name__ == "__main__":
    rule1 = Condition(
        Field("email_from"), Predicate("equals"), "founders@dailycodingproblem.com"
    )
    rule2 = Condition(Field("subject"), Predicate("contains"), "coding")
    rule3 = Condition(Field("received_date"), Predicate("less_than"), "10 days")
    rule_groups = [rule1, rule2, rule3]
    action1 = Action("read")
    action2 = Action("inbox")
    action_groups = [action1, action2]
    obj = ConditionActionGroup(rule_groups, "any", action_groups)
    obj.process_emails()
