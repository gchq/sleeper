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

import pytest
from mypy_boto3_sqs.service_resource import Queue
from pytest_mock import MockerFixture

from sleeper import SleeperClient
from sleeper.properties import CommonCdkProperty, InstanceProperties, QueryCdkProperty, save_instance_properties
from sleeper.query import Query, QuerySender, Range, Region
from tests.sleeper.local_aws import LocalAws
from tests.sleeper.local_aws_sleeper_client import LocalAwsSleeperClient
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


def should_send_exact_query_by_dict_with_client(sleeper_client: SleeperClient, query_queue_resource: Queue, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    keys = {"key": ["my_key", "my_key2"]}

    mocked_results = ["mocked row 1", "mocked row 2"]
    # Patch the _receive_messages function in the module sleeper.client as this is not tested in this case.
    mocker.patch("sleeper.client._receive_messages", return_value=mocked_results)

    # When
    # We don't care about the results for this test as they are mocked.
    sleeper_client.exact_key_query(table_name=table_name, keys=keys, query_id=query_id)

    # Then
    expected_message_json = [
        {
            "queryId": "test-query",
            "tableName": "test-table",
            "type": "Query",
            "regions": [
                {"key": {"min": "my_key", "minInclusive": True, "max": "my_key", "maxInclusive": True}, "stringsBase64Encoded": False},
                {"key": {"min": "my_key2", "minInclusive": True, "max": "my_key2", "maxInclusive": True}, "stringsBase64Encoded": False},
            ],
        }
    ]

    messages = receive_messages(query_queue_resource)

    assert messages == expected_message_json


def should_send_exact_query_by_list_with_client(sleeper_client: SleeperClient, query_queue_resource: Queue, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    keys = [{"key1": "value1", "key2": "value2"}, {"key1": "valueA", "key2": "valueB"}]

    mocked_results = ["mocked row 1", "mocked row 2"]
    # Patch the _receive_messages function in the module sleeper.client as this is not tested in this case.
    mocker.patch("sleeper.client._receive_messages", return_value=mocked_results)

    # When
    # We don't care about the results for this test as they are mocked.
    sleeper_client.exact_key_query(table_name=table_name, keys=keys, query_id=query_id)

    # Then
    expected_message_json = [
        {
            "queryId": "test-query",
            "tableName": "test-table",
            "type": "Query",
            "regions": [
                {
                    "key1": {"min": "value1", "minInclusive": True, "max": "value1", "maxInclusive": True},
                    "key2": {"min": "value2", "minInclusive": True, "max": "value2", "maxInclusive": True},
                    "stringsBase64Encoded": False,
                },
                {
                    "key1": {"min": "valueA", "minInclusive": True, "max": "valueA", "maxInclusive": True},
                    "key2": {"min": "valueB", "minInclusive": True, "max": "valueB", "maxInclusive": True},
                    "stringsBase64Encoded": False,
                },
            ],
        }
    ]

    messages = receive_messages(query_queue_resource)

    assert messages == expected_message_json


def should_send_range_query_with_client(sleeper_client: SleeperClient, query_queue_resource: Queue, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    regions = [{"key": ["my-key", "my-keys"]}, {"key": ["key", "keys"]}]
    mocked_results = ["mocked row 1", "mocked row 2"]
    # Patch the _receive_messages function in the module sleeper.client as this is not tested in this case.
    mocker.patch("sleeper.client._receive_messages", return_value=mocked_results)

    # When
    # We don't care about the results for this test as they are mocked.
    sleeper_client.range_key_query(table_name=table_name, regions=regions, query_id=query_id)

    # Then
    expected_message_json = [
        {
            "queryId": "test-query",
            "tableName": "test-table",
            "type": "Query",
            "regions": [
                {"key": {"min": "my-key", "minInclusive": True, "max": "my-keys", "maxInclusive": False}, "stringsBase64Encoded": False},
                {"key": {"min": "key", "minInclusive": True, "max": "keys", "maxInclusive": False}, "stringsBase64Encoded": False},
            ],
        }
    ]

    messages = receive_messages(query_queue_resource)

    assert messages == expected_message_json


@pytest.mark.asyncio
async def should_send_exact_query_with_client_via_web_socket(sleeper_client: SleeperClient, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    keys = {"key": ["my_key"]}
    mocked_results = ["mocked row 1"]
    mock_process = mocker.patch("sleeper.web_socket_query.WebSocketQueryProcessor.process_query", autospec=True, return_value=mocked_results)

    # When
    # We don't care about the results for this test as they are mocked.
    await sleeper_client.web_socket_exact_key_query(table_name=table_name, keys=keys, query_id=query_id)

    # Then
    expected_query = Query(table_name=table_name, query_id=query_id, regions=[Region(row_key_field_to_range={"key": Range.exact_value("my_key")})])
    assert [call.args[1].to_dict() for call in mock_process.call_args_list] == [expected_query.to_dict()]


@pytest.mark.asyncio
async def should_send_range_query_with_client_via_web_socket(sleeper_client: SleeperClient, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    keys = [{"key": {"min": "a", "max": "z"}}]
    mocked_results = ["mocked row 1", "mocked row 2"]
    mock_process = mocker.patch("sleeper.web_socket_query.WebSocketQueryProcessor.process_query", autospec=True, return_value=mocked_results)

    # When
    # We don't care about the results for this test as they are mocked.
    await sleeper_client.web_socket_range_key_query(table_name=table_name, keys=keys, query_id=query_id)

    # Then
    expected_region = Region(row_key_field_to_range={"key": Range(min="a", max="z", min_inclusive=True, max_inclusive=True)})
    expected_query = Query(table_name=table_name, query_id=query_id, regions=[expected_region])
    assert [call.args[1].to_dict() for call in mock_process.call_args_list] == [expected_query.to_dict()]


def should_send_exact_query_with_sql_by_dict_with_client(sleeper_client: SleeperClient, query_queue_resource: Queue, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    keys = {"key": ["my_key"]}
    sql_query = "SELECT * FROM query_results WHERE value > 10"

    mocked_results = ["mocked row 1"]
    mocker.patch("sleeper.client._receive_messages", return_value=mocked_results)

    # When
    sleeper_client.exact_key_query(table_name=table_name, keys=keys, query_id=query_id, sql_query=sql_query)

    # Then
    expected_message_json = [
        {
            "queryId": "test-query",
            "tableName": "test-table",
            "type": "Query",
            "regions": [
                {"key": {"min": "my_key", "minInclusive": True, "max": "my_key", "maxInclusive": True}, "stringsBase64Encoded": False},
            ],
            "sqlQuery": "SELECT * FROM query_results WHERE value > 10",
        }
    ]

    messages = receive_messages(query_queue_resource)

    assert messages == expected_message_json


def should_send_range_query_with_sql_with_client(sleeper_client: SleeperClient, query_queue_resource: Queue, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    regions = [{"key": ["my-key", "my-keys"]}]
    sql_query = "SELECT key, COUNT(*) FROM query_results GROUP BY key"

    mocked_results = ["mocked row 1"]
    mocker.patch("sleeper.client._receive_messages", return_value=mocked_results)

    # When
    sleeper_client.range_key_query(table_name=table_name, regions=regions, query_id=query_id, sql_query=sql_query)

    # Then
    expected_message_json = [
        {
            "queryId": "test-query",
            "tableName": "test-table",
            "type": "Query",
            "regions": [
                {"key": {"min": "my-key", "minInclusive": True, "max": "my-keys", "maxInclusive": False}, "stringsBase64Encoded": False},
            ],
            "sqlQuery": "SELECT key, COUNT(*) FROM query_results GROUP BY key",
        }
    ]

    messages = receive_messages(query_queue_resource)

    assert messages == expected_message_json


@pytest.mark.asyncio
async def should_send_exact_query_with_sql_via_web_socket(sleeper_client: SleeperClient, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    keys = {"key": ["my_key"]}
    sql_query = "SELECT * FROM query_results WHERE value > 100"
    mocked_results = ["mocked row 1"]
    mock_process = mocker.patch("sleeper.web_socket_query.WebSocketQueryProcessor.process_query", autospec=True, return_value=mocked_results)

    # When
    await sleeper_client.web_socket_exact_key_query(table_name=table_name, keys=keys, query_id=query_id, sql_query=sql_query)

    # Then
    expected_query = Query(table_name=table_name, query_id=query_id, regions=[Region(row_key_field_to_range={"key": Range.exact_value("my_key")})], sql_query=sql_query)
    assert [call.args[1].to_dict() for call in mock_process.call_args_list] == [expected_query.to_dict()]


def should_upload_query_too_large_for_sqs_message_with_client(sleeper_client: SleeperClient, query_queue_resource: Queue, properties: InstanceProperties, mocker: MockerFixture):
    # Given
    mocker.patch("sleeper.client._receive_messages", return_value=[])
    keys = {"key": [f"key-{i}" for i in range(5000)]}
    query = Query(query_id="large-query", table_name="test-table", regions=Region.list_from_field_to_exact_values(keys))
    assert len(query.to_json()) > 256 * 1024

    # When
    sleeper_client.exact_key_query(table_name="test-table", keys=keys, query_id="large-query")

    # Then
    messages = receive_messages(query_queue_resource)
    assert len(messages) == 1
    message = messages[0]
    assert message.keys() == {"queryId", "queryJsonS3Key"}
    assert message["queryId"] == "large-query"
    assert message["queryJsonS3Key"].startswith("queries/large-query/query-")
    assert read_stored_query(properties, message["queryJsonS3Key"]) == query.to_dict()


def should_send_query_in_message_when_below_size_limit(query_queue_resource: Queue, properties: InstanceProperties):
    # Given
    query = Query(query_id="test-query", table_name="test-table", regions=[Region.exact_value("key", "a")])
    sender = QuerySender(LocalAws.sqs_resource(), LocalAws.s3_client(), properties, json_length_to_store=len(query.to_json()) + 1)

    # When
    sender.send(query)

    # Then
    assert receive_messages(query_queue_resource) == [query.to_dict()]
    assert list_stored_queries(properties) == []


def should_upload_query_when_at_size_limit(query_queue_resource: Queue, properties: InstanceProperties):
    # Given
    query = Query(query_id="test-query", table_name="test-table", regions=[Region.exact_value("key", "a")])
    sender = QuerySender(LocalAws.sqs_resource(), LocalAws.s3_client(), properties, json_length_to_store=len(query.to_json()))

    # When
    sender.send(query)

    # Then
    [message] = receive_messages(query_queue_resource)
    assert message == {"queryId": "test-query", "queryJsonS3Key": message["queryJsonS3Key"]}
    assert list_stored_queries(properties) == [message["queryJsonS3Key"]]
    assert read_stored_query(properties, message["queryJsonS3Key"]) == query.to_dict()


@pytest.fixture
def sleeper_client(properties: InstanceProperties) -> SleeperClient:
    LocalAws.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalAws.s3_resource(), properties)
    return LocalAwsSleeperClient.create(properties)


@pytest.fixture
def properties(query_queue_resource: Queue) -> InstanceProperties:
    properties = create_test_instance_properties()
    properties.set(QueryCdkProperty.QUERY_QUEUE_URL, query_queue_resource.url)
    properties.set(QueryCdkProperty.QUERY_RESULTS_BUCKET, LocalAws.create_bucket().name)
    properties.set(QueryCdkProperty.QUERY_WEBSOCKET_URL, "ws://localhost:7777/_aws/ws/api_id/test")
    properties.set(CommonCdkProperty.REGION, "eu-west-2")
    return properties


@pytest.fixture
def query_queue_resource() -> Queue:
    return LocalAws.create_queue()


def receive_messages(queue_resource: Queue):
    messages = queue_resource.receive_messages(WaitTimeSeconds=0)
    return [json.loads(message.body) for message in messages]


def list_stored_queries(properties: InstanceProperties) -> list[str]:
    bucket = LocalAws.s3_resource().Bucket(properties.get(QueryCdkProperty.QUERY_RESULTS_BUCKET))
    return [obj.key for obj in bucket.objects.filter(Prefix="queries/")]


def read_stored_query(properties: InstanceProperties, key: str) -> dict:
    response = LocalAws.s3_client().get_object(Bucket=properties.get(QueryCdkProperty.QUERY_RESULTS_BUCKET), Key=key)
    return json.loads(response["Body"].read())
