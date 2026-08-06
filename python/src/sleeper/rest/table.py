#  Copyright 2022-2026 Crown Copyright
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
