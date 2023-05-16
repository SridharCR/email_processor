from sqlalchemy import select
from sqlalchemy.orm import Session

from lib.constants import FIELD_MAP, PREDICATE_MAP
from lib.db import postgresql_engine
from lib.log import logger
from src.rule_processor.email_db import EmailMetadata, EmailBody
from src.rule_processor.email_loader import do_actions


class Field:
    def __init__(self, field):
        if not FIELD_MAP.get(field):
            logger.warning(
                f"{field} is not configured yet, kindly try with different field"
            )
            raise
        self.field = field
        self.datatype = FIELD_MAP.get(field)


class Predicate:
    def __init__(self, predicate):
        if not PREDICATE_MAP.get(predicate):
            logger.warning(
                f"{predicate} is not configured yet, kindly try with different predicate"
            )
            raise
        self.predicate = predicate
        self.datatype = PREDICATE_MAP.get(predicate)
        self.method = getattr(self, predicate)

    def contains(self, key, value):
        return key.like("%{}%".format(value))

    def does_not_contains(self, key, value):
        return not key.like("%{}%".format(value))

    def equals(self, key, value):
        return key == value

    def does_not_equals(self, key, value):
        return key != value


class Rule:
    def __init__(self, field, predicate, value):
        self.field_obj = field
        self.predicate_obj = predicate
        self.value = value
        self.validate()
        self.result = []

    def validate(self):
        pass

    def generate_where_clause(self):
        try:
            key = EmailMetadata.getattr(self.field_obj.field)
            where_statement = self.predicate_obj.method(key, self.value)
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


class RuleActionGroup:
    def __init__(self, rules, rule_group_predicate, actions):
        self.rules = rules
        self.rule_group_predicate = rule_group_predicate
        self.actions = actions
        self.result = []

    def rule(self):
        pass

    def rule_cleanups(self):
        pass

    def filter_data(self):
        session = None
        try:
            session = Session(postgresql_engine)
            cumulative_where_clause = None
            for each_rule in self.rules:
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
            statement = select(EmailMetadata).filter(cumulative_where_clause)
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

    def apply_action(self):
        for each_action in self.actions:
            do_actions(self.result, each_action.payload)


if __name__ == "__main__":
    rule1 = Rule(Field("subject"), Predicate("equals"), "Fwd: HappyFox - Assignment")
    rule2 = Rule(Field("subject"), Predicate("contains"), "Daily")
    rule3 = Rule(Field("subject"), Predicate("contains"), "followed")
    rule_groups = [rule1, rule2, rule3]
    action1 = Action("unread")
    action2 = Action("inbox")
    action_groups = [action1, action2]
    obj = RuleActionGroup(rule_groups, "any", action_groups)
    obj.filter_data()
    obj.apply_action()
