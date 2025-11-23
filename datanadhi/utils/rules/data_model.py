from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, RootModel


class ConditionType(str, Enum):
    EXACT = "exact"
    PARTIAL = "partial"
    REGEX = "regex"


# Raw Data Models
class RawCondition(BaseModel, frozen=True):
    key: str
    type: ConditionType
    negate: bool | None = None
    value: str


class RawRule(BaseModel):
    name: str | None = None
    any_condition_match: bool | None = None
    conditions: list[RawCondition]
    stdout: bool | None = None
    pipelines: tuple[str, ...] | None = None


class RawAction(BaseModel, frozen=True):
    stdout: bool | None = None
    pipelines: tuple[str, ...] | None = None


# Data Models
class Condition(BaseModel):
    key: str
    type: ConditionType
    negate: bool = False
    value: str


class Action(BaseModel):
    stdout: bool = False
    pipelines: set[str] = []


class Rule(BaseModel):
    any_condition_match: bool = False
    conditions: list[Condition]


class Rules(RootModel[list[Rule]]):
    root: list[Rule] = []

    def __iter__(self):
        return iter(self.root)

    def __bool__(self):
        return len(self.root) > 0


class RuleAction(BaseModel):
    action: Action
    rules: Rules


class RuleActions(RootModel[list[RuleAction]]):
    root: list[RuleAction] = []

    def __iter__(self):
        for action_rule in self.root:
            yield (action_rule.action, action_rule.rules)

    def __bool__(self):
        return len(self.root) > 0


@dataclass
class RuleEvaluationResult:
    """Result of evaluating rules against a log entry.

    Args:
        pipelines: List of pipeline IDs that should be triggered
        stdout: Whether the log should be output to stdout
        payload: The complete log data that triggered the rule
    """

    pipelines: list[str]
    stdout: bool
    payload: dict
