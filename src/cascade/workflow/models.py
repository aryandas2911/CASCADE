from dataclasses import dataclass, field
from typing import Any


@dataclass
class Task:
    id: str
    dependencies: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class Workflow:
    id: str
    name: str
    tasks: list[Task] = field(default_factory=list)
