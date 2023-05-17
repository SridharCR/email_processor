from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from lib.constants import FIELD_MAP, PREDICATE_MAP
from lib.db import postgresql_engine
from lib.log import logger
from src.rule_processor.dao.email_db import EmailMetadata, EmailBody
from src.rule_processor.middlewares.gmail_apis import GmailApi


class Field:
    def __init__(self, field):
        self.field = field
        self.datatype = FIELD_MAP.get(field)
        self.valid_flag = self.is_valid()


    def is_valid(self):
        if not FIELD_MAP.get(self.field):
            logger.warning(
                f"{self.field} is not configured yet, kindly try with different field"
            )
            return False
        return True


class Predicate:
    def __init__(self, predicate):
        self.predicate = predicate
        self.datatype = PREDICATE_MAP.get(predicate)
        self.method = getattr(self, predicate) if hasattr(self, predicate) else None
        self.valid_flag = self.is_valid()


    def is_valid(self):
        if not PREDICATE_MAP.get(self.predicate):
            logger.warning(
                f"{self.predicate} is not configured yet, kindly try with different predicate"
            )
            return False
        return True

    def contains(self, key, value, **kwargs):
        return key.like("%{}%".format(value))

    def does_not_contains(self, key, value, **kwargs):
        return not key.like("%{}%".format(value))

    def equals(self, key, value, **kwargs):
        return key == value

    def does_not_equals(self, key, value, **kwargs):
        return key != value

    def less_than(self, key, value, **kwargs):
        args = {kwargs["time_entity"] : value}
        range = datetime.now().today() - timedelta(**args)
        return key > range

    def more_than(self, key, value, **kwargs):
        args = {kwargs["time_entity"] : value}
        range = datetime.now().today() - timedelta(**args)
        return key < range

class Condition:
    def __init__(self, field, predicate, value):
        self.field_obj = field
        self.predicate_obj = predicate
        self.value = value
        self.validate()
        self.result = []

    def validate(self):
        if self.field_obj.datatype == datetime:
            val = self.value.split(' ')
            self.value = val[0]
            self.time_entity = val[1]

            if self.time_entity not in ['days', 'seconds', 'minutes', 'hours', 'weeks', 'months']:
                raise Exception("Invalid time config")

            if self.time_entity == 'months':
                self.value = self.value * 30
                self.time_entity = 'days'

    def generate_where_clause(self):
        try:
            key = EmailMetadata.getattr(self.field_obj.field) or EmailBody.getattr(self.field_obj.field)
            where_statement = self.predicate_obj.method(key, int(self.value), time_entity=self.time_entity)
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
    def __init__(self, conditions, rule_group_predicate, actions):
        self.conditions = conditions
        self.rule_group_predicate = rule_group_predicate
        self.actions = actions
        self.result = []

    def process_emails(self):
        self.__filter_data()
        self.__apply_action()

    def __filter_data(self):
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
            print(statement)
            rows = session.execute(statement).all()
            for each in rows:
                self.result.append(each[0].id)
            self.result = list(set(self.result))
        except Exception as ex:
            session.rollback()
            session.close()
            logger.exception("Error occurred while preparing the filter query")
            raise

    def __apply_action(self):
        if self.result:
            logger.info(f"{len(self.result)} Matching emails found")
            for each_action in self.actions:
                payload = each_action.payload
                payload["ids"] = self.result
                gmail_obj = GmailApi()
                gmail_obj.do_actions(each_action.payload)
        else:
            logger.info("No matching emails found, hence skipping")


def option_builder():
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
            field = Field(input("Select a field from the given list: "))
            field_valid_flag = field.valid_flag

        predicate_valid_flag = False
        predicate = None
        while not predicate_valid_flag:
            predicate = Predicate(input("Select a predicate from the given list: "))
            predicate_valid_flag = predicate.valid_flag

        value = input("Enter the value: ")

        condition = Condition(field, predicate, value)
        rule_list.append(condition)
        new_rule_flag = input("Do you want to add another condition? (Yes/No):")
        new_rule_flag = True if new_rule_flag == "Yes" else False

    while new_action_flag:
        action = Action(input("Select an action: "))
        action_list.append(action)
        new_action_flag = input("Do you want to add another action? (Yes/No):")
        new_action_flag = True if new_action_flag == "Yes" else False

    object = ConditionActionGroup(rule_list, rule_predicate, action_list)
    object.process_emails()


if __name__ == "__main__":
    rule1 = Condition(Field("subject"), Predicate("equals"), "Fwd: HappyFox - Assignment")
    rule2 = Condition(Field("message"), Predicate("contains"), "Daily")
    rule3 = Condition(Field("subject"), Predicate("contains"), "followed")
    rule_groups = [rule1, rule2, rule3]
    action1 = Action("unread")
    action2 = Action("inbox")
    action_groups = [action1, action2]
    obj = ConditionActionGroup(rule_groups, "any", action_groups)
    obj.process_emails()
