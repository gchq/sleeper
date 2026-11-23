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
import logging
import uuid
from itertools import chain

from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSServiceResource

from sleeper.properties import InstanceProperties, QueryCdkProperty

logger = logging.getLogger(__name__)

DEFAULT_JSON_LENGTH_TO_STORE = 1024 * 200
"""
Minimum size in bytes of a query's JSON that will be written to S3 rather than sent in an SQS message.

The maximum size of an SQS message is 256KiB. Setting this to less than that leaves room for message attributes.
This matches the threshold used by the Java clients.
"""

STORED_QUERY_KEY_FIELD = "queryJsonS3Key"


class Range:
    def __init__(self, min=None, min_inclusive=True, max=None, max_inclusive=False):
        if min is None:
            raise ValueError("min must be specified")
        if not isinstance(min_inclusive, bool):
            raise TypeError("min_inclusive must be a bool")
        if not isinstance(max_inclusive, bool):
            raise TypeError("max_inclusive must be a bool")
        self.min = min
        self.min_inclusive = min_inclusive
        self.max = max
        self.max_inclusive = max_inclusive

    @staticmethod
    def exact_value(value):
        return Range(min=value, min_inclusive=True, max=value, max_inclusive=True)

    @staticmethod
    def from_tuple(value):
        if len(value) == 2:
            return Range(min=value[0], max=value[1])
        if len(value) == 4:
            return Range(min=value[0], min_inclusive=value[1], max=value[2], max_inclusive=value[3])
        raise ValueError("Expected a tuple of length 2 or 4")

    @staticmethod
    def from_dict(value, min_inclusive=True, max_inclusive=True):
        return Range(value["min"], min_inclusive, value["max"], max_inclusive)

    def to_dict(self):
        return {"min": self.min, "minInclusive": self.min_inclusive, "max": self.max, "maxInclusive": self.max_inclusive}


class Region:
    def __init__(self, row_key_field_to_range: dict[str, Range] | None = None, strings_base64_encoded: bool = False):
        if not isinstance(row_key_field_to_range, dict):
            raise TypeError("row_key_field_to_range must be a dictionary")
        if len(row_key_field_to_range) == 0:
            raise ValueError("Must provide at least one range")
        if not isinstance(strings_base64_encoded, bool):
            raise TypeError("strings_base64_encoded must be a bool")
        self.row_key_field_to_range = row_key_field_to_range
        self.strings_base64_encoded = strings_base64_encoded

    @staticmethod
    def exact_value(field: str, value, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.exact_value(value)}, strings_base64_encoded=strings_base64_encoded)

    @staticmethod
    def from_field_to_exact_value(field_to_value: dict, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.exact_value(value) for field, value in field_to_value.items()}, strings_base64_encoded=strings_base64_encoded)

    @staticmethod
    def list_from_field_to_exact_values(field_to_values: dict, strings_base64_encoded: bool = False):
        return list(chain.from_iterable((Region.exact_value(item[0], value, strings_base64_encoded) for value in item[1]) for item in field_to_values.items()))

    @staticmethod
    def from_field_to_tuple(field_to_tuple: dict, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.from_tuple(value) for field, value in field_to_tuple.items()}, strings_base64_encoded=strings_base64_encoded)

    @staticmethod
    def from_field_to_dict(field_to_dict: dict, min_inclusive: bool = True, max_inclusive: bool = True, strings_base64_encoded: bool = False):
        return Region(row_key_field_to_range={field: Range.from_dict(value, min_inclusive, max_inclusive) for field, value in field_to_dict.items()}, strings_base64_encoded=strings_base64_encoded)

    def to_dict(self):
        out = {field: range.to_dict() for field, range in self.row_key_field_to_range.items()}
        out["stringsBase64Encoded"] = self.strings_base64_encoded
        return out


class Query:
    def __init__(self, query_id: str | None = None, table_name: str | None = None, regions: list[Region] | None = None, sql_query: str | None = None):
        if query_id is None:
            query_id = str(uuid.uuid4())
        elif not isinstance(query_id, str):
            raise TypeError("query_id must be a string")
        if not isinstance(table_name, str):
            raise TypeError("table_name must be a string")
        if not isinstance(regions, list):
            raise TypeError("regions must be a list")
        if len(regions) == 0:
            raise ValueError("Must provide at least one region")
        if sql_query is not None and not isinstance(sql_query, str):
            raise TypeError("sql_query must be a string")
        self.query_id = query_id
        self.table_name = table_name
        self.regions = regions
        self.sql_query = sql_query

    def to_dict(self):
        regions = [region.to_dict() for region in self.regions]
        value = {"tableName": self.table_name, "queryId": self.query_id, "type": "Query", "regions": regions}
        if self.sql_query is not None:
            value["sqlQuery"] = self.sql_query
        return value

    def to_json(self):
        return json.dumps(self.to_dict())


class QuerySender:
    """
    Sends queries to the query queue, to be executed in a lambda. If a query is too large to fit in an SQS message,
    it is written to the query results bucket in S3, and the message only holds the query ID and the S3 key.
    """

    def __init__(
        self,
        sqs: SQSServiceResource,
        s3_client: S3Client,
        properties: InstanceProperties,
        json_length_to_store: int = DEFAULT_JSON_LENGTH_TO_STORE,
    ):
        self.sqs = sqs
        self.s3_client = s3_client
        self.properties = properties
        self.json_length_to_store = json_length_to_store

    def send(self, query: Query):
        message = self._upload_if_too_big(query)
        queue_url = self.properties.get(QueryCdkProperty.QUERY_QUEUE_URL)
        self.sqs.Queue(queue_url).send_message(MessageBody=message)
        logger.debug(f"Submitted query with id {query.query_id}")

    def _upload_if_too_big(self, query: Query) -> str:
        query_json = query.to_json()
        query_bytes = query_json.encode("utf-8")
        if len(query_bytes) < self.json_length_to_store:
            return query_json
        key = _create_stored_query_key(query.query_id)
        bucket = self.properties.get(QueryCdkProperty.QUERY_RESULTS_BUCKET)
        logger.info(f"Query JSON of {len(query_bytes)} bytes is too large for a message, uploading to s3://{bucket}/{key}")
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=query_bytes)
        return json.dumps({"queryId": query.query_id, STORED_QUERY_KEY_FIELD: key})


def _create_stored_query_key(query_id: str) -> str:
    return f"queries/{query_id}/query-{uuid.uuid4()}.json"
