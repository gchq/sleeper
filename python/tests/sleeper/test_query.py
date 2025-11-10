import json
from unittest.mock import ANY, call

import pytest
from mypy_boto3_sqs.service_resource import Queue
from pytest_mock import MockerFixture

from sleeper.client import SleeperClient
from sleeper.properties.cdk_defined_properties import CommonCdkProperty, QueryCdkProperty
from sleeper.properties.config_bucket import save_instance_properties
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.web_socket_query import WebSocketQuery
from tests.sleeper.localstack import LocalStack
from tests.sleeper.localstack_sleeper_client import LocalStackSleeperClient
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


class MatchesQueryJson:
    def __init__(self, expected_json: str):
        self.expected_json = expected_json
    def __eq__(self, other) -> bool:
        try:
            return other.to_json() == self.expected_json
        except Exception:
            return False

def should_send_exact_query_with_client(sleeper_client: SleeperClient, query_resource: Queue, mocker: MockerFixture):
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

    messages = receive_messages(query_resource)

    assert messages == expected_message_json


def should_send_range_query_with_client(sleeper_client: SleeperClient, query_resource: Queue, mocker: MockerFixture):
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

    messages = receive_messages(query_resource)

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

    expected_query = WebSocketQuery(table_name=table_name, query_id=query_id, key="key", max_value="my_key", min_value="my_key", strings_base64_encoded=False).to_json()

    # Then
    assert mock_process.call_args_list == [call(ANY, MatchesQueryJson(expected_query))]

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

    expected_query = WebSocketQuery(table_name=table_name, query_id=query_id, key="key", max_value="z", min_value="a", strings_base64_encoded=False).to_json()

    # Then
    assert mock_process.call_args_list == [call(ANY, MatchesQueryJson(expected_query))]


@pytest.fixture
def sleeper_client(properties: InstanceProperties) -> SleeperClient:
    LocalStack.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalStack.s3_resource(), properties)
    return LocalStackSleeperClient.create(properties)


@pytest.fixture
def properties(query_resource: Queue) -> InstanceProperties:
    properties = create_test_instance_properties()
    properties.set(QueryCdkProperty.QUERY_QUEUE_URL, query_resource.url)
    properties.set(QueryCdkProperty.QUERY_WEBSOCKET_URL, "ws://localhost:7777/_aws/ws/api_id/test")
    properties.set(CommonCdkProperty.REGION, "eu-west-2")
    return properties


@pytest.fixture
def query_resource() -> Queue:
    return LocalStack.create_queue()


def receive_messages(queue_resource: Queue):
    messages = queue_resource.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
