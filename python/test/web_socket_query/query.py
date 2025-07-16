#  Copyright 2022-2025 Crown Copyright
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
import uuid


class Query:
    """
    Represents a Sleeper query with specific parameters, capable of serializing itself into JSON for WebSocket communication.

    Attributes:
        query_id (str): Unique identifier for the query instance.
        table_name (str): Name of the database table to query.
        key (str): The key or column to filter on.
        min_value (str): The minimum value for the query region.
        max_value (str): The maximum value for the query region.
        min_inclusive (bool): Whether the minimum bound is inclusive.
        max_inclusive (bool): Whether the maximum bound is inclusive.
        strings_base64_encoded (bool): Whether string values are base64 encoded.
    """

    def __init__(
        self,
        table_name: str,
        key: str,
        min_value: str,
        max_value: str,
        min_inclusive: bool,
        max_inclusive: bool,
        strings_base64_encoded: bool,
    ):
        """
        Initializes a new Query object with specified parameters.

        Args:
            table_name (str): The name of the database table to query.
            key (str): The key or column to filter on.
            min_value (str): The minimum value for the query region.
            max_value (str): The maximum value for the query region.
            min_inclusive (bool): Whether the minimum bound is inclusive.
            max_inclusive (bool): Whether the maximum bound is inclusive.
            strings_base64_encoded (bool): Whether strings are encoded in base64.
        """
        self.query_id = str(uuid.uuid4())
        self.table_name = table_name
        self.key = key
        self.min_value = min_value
        self.max_value = max_value
        self.min_inclusive = min_inclusive
        self.max_inclusive = max_inclusive
        self.strings_base64_encoded = strings_base64_encoded

    def to_json(self):
        """
        Converts the Query object into a JSON-formatted string suitable for sending over WebSocket.

        Returns:
            str: JSON representation of the query object.
        """
        regions = [
            {
                "key": {
                    "min": self.min_value,
                    "minInclusive": self.min_inclusive,
                    "max": self.max_value,
                    "maxInclusive": self.max_inclusive,
                },
                "stringsBase64Encoded": self.strings_base64_encoded,
            }
        ]
        obj = {
            "queryId": self.query_id,
            "type": "Query",
            "tableName": self.table_name,
            "regions": regions,
            "resultsPublisherConfig": {},
        }
        return json.dumps(obj)
