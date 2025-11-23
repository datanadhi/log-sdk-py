import glob
import os
from pathlib import Path

from pydantic import ValidationError

from datanadhi.utils.files import load_from_yaml, read_from_json, write_to_json
from datanadhi.utils.rules.data_model import (
    RawAction,
    RawCondition,
    RawRule,
    RuleActions,
)


class ResolvedRules:
    def __init__(self, datanadhi_dir: Path):
        self.datanadhi_dir = datanadhi_dir
        self.rules = {}
        self.path = datanadhi_dir / ".rules.resolved.json"

    @staticmethod
    def get_rule_dict(rule: set[RawCondition]):
        return [cond.model_dump(exclude_none=True) for cond in rule]

    def remove_none_or_defaults(self, rule: RawRule):
        if rule.stdout is False:
            rule.stdout = None
        if rule.pipelines == ():
            rule.pipelines = None
        if rule.any_condition_match is False:
            rule.any_condition_match = None
        if len(rule.conditions) == 1:
            rule.any_condition_match = True
        return rule

    def get_structured_rule(self, rule):
        try:
            rule = RawRule(**rule)
            self.remove_none_or_defaults(rule)

            if rule.pipelines is None and rule.stdout is None:
                return None

            if len(rule.conditions) == 0:
                return None

            return rule
        except ValidationError as e:
            print(e)
            return None

    def add_rule(self, rule: RawRule):
        action = RawAction(stdout=rule.stdout, pipelines=rule.pipelines)
        if action not in self.rules:
            self.rules[action] = {"or": set(), "other": set()}

        if rule.any_condition_match:
            self.rules[action]["or"].update(rule.conditions)
        else:
            self.rules[action]["other"].add(frozenset(rule.conditions))

    def get_rules_from_file(self, path: Path):
        data = load_from_yaml(path)
        if not isinstance(data, list):
            return
        for rule in data:
            modified_rule = self.get_structured_rule(rule)
            if modified_rule:
                self.add_rule(modified_rule)

    def action_rule_to_json(self):
        rules_json = []
        for action, rules in self.rules.items():
            rule_json = {"action": {}, "rules": []}
            if action.stdout:
                rule_json["action"]["stdout"] = action.stdout
            if action.pipelines:
                rule_json["action"]["pipelines"] = list(action.pipelines)

            if "or" in rules:
                rule_json["rules"].append(
                    {
                        "any_condition_match": True,
                        "conditions": self.get_rule_dict(rules["or"]),
                    }
                )

            if "other" in rules:
                for and_rule in rules["other"]:
                    rule_json["rules"].append(
                        {"conditions": self.get_rule_dict(and_rule)}
                    )
            rules_json.append(rule_json)
        return rules_json

    def build_rules_from_files(self):
        paths = glob.glob(f"{self.datanadhi_dir}/rules/*.yaml")
        paths += glob.glob(f"{self.datanadhi_dir}/rules/*.yml")

        for path in paths:
            self.get_rules_from_file(path)

        action_rules = self.action_rule_to_json()
        write_to_json(self.path, action_rules)
        return action_rules

    def get(self):
        if os.path.exists(self.path):
            return RuleActions(read_from_json(self.path))
        return RuleActions(self.build_rules_from_files())
