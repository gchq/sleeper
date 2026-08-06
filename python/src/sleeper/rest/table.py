import json
from dataclasses import asdict, dataclass

import requests


@dataclass
class TableSchema:
    rowKeyFields: list[dict[str, str]]
    sortKeyFields: list[dict[str, str]]
    valueFields: list[dict[str, str]]


@dataclass
class AddTableResponse:
    tableId: str
    tableName: str

    @classmethod
    def from_dict(cls, data: dict) -> "AddTableResponse":
        return cls(**data)

    @classmethod
    def from_response(
        cls,
        response: requests.Response,
    ) -> "AddTableResponse":
        return cls.from_dict(response.json())


@dataclass
class AddTableRequest:
    properties: dict[str, str]
    schema: TableSchema

    splitPoints: list[str] | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))
