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
    """
    Schema definition for a Sleeper table.

    Defines the row key, sort key and value fields used to store records.
    """

    rowKeyFields: list[dict[str, str]]
    sortKeyFields: list[dict[str, str]]
    valueFields: list[dict[str, str]]


@dataclass
class AddTableResponse:
    """Response returned after successfully creating a table."""

    tableId: str
    tableName: str

    @classmethod
    def from_dict(cls, data: dict) -> "AddTableResponse":
        """
        Create an AddTableResponse from a dictionary.

        :param data: Dictionary containing the response data.
        :return: The deserialised response object.
        """
        return cls(**data)

    @classmethod
    def from_response(cls, response: requests.Response) -> "AddTableResponse":
        """
        Create an AddTableResponse from an HTTP response.

        :param response: The HTTP response returned by the REST API.
        :return: The deserialised response object.
        """
        return cls.from_dict(response.json())


@dataclass
class AddTableRequest:
    """Request payload for creating a Sleeper table."""

    properties: dict[str, str]
    schema: TableSchema
    splitPoints: list[str] | None = None

    def to_json(self) -> str:
        """Serialise the request payload to a JSON string.

        :returns: str: The request payload as JSON.
        """
        return json.dumps(asdict(self))
