import json

import pytest
from mypy_boto3_dynamodb.service_resource import Table
from mypy_boto3_sqs.service_resource import Queue
from pytest_mock import MockerFixture

from sleeper.client import SleeperClient
from sleeper.properties.cdk_defined_properties import CommonCdkProperty, QueryCdkProperty
from sleeper.properties.config_bucket import save_instance_properties
from sleeper.properties.instance_properties import InstanceProperties
from tests.sleeper.localstack import LocalStack
from tests.sleeper.localstack_sleeper_client import LocalStackSleeperClient
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


def should_send_extact_query_with_client(sleeper_client: SleeperClient, query: Queue, mocker: MockerFixture):
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

    messages = receive_messages(query)

    assert messages == expected_message_json


def should_send_range_query_with_client(sleeper_client: SleeperClient, query: Queue, mocker: MockerFixture):
    # Given
    table_name = "test-table"
    query_id = "test-query"
    regions = [{'key': ["my-key", "my-keys"]}, {'key': ["key", "keys"]}]
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

    messages = receive_messages(query)

    assert messages == expected_message_json


@pytest.fixture
def sleeper_client(properties: InstanceProperties) -> SleeperClient:
    LocalStack.create_bucket(properties.get(CommonCdkProperty.CONFIG_BUCKET))
    save_instance_properties(LocalStack.s3_resource(), properties)
    return LocalStackSleeperClient.create(properties)


@pytest.fixture
def properties(query: Queue, table: Table) -> InstanceProperties:
    properties = create_test_instance_properties()
    properties.set(QueryCdkProperty.QUERY_QUEUE_URL, query.url)
    properties.set(QueryCdkProperty.QUERY_TRACKER_TABLE, table.table_name)
    return properties


@pytest.fixture
def query() -> Queue:
    return LocalStack.create_queue()


@pytest.fixture
def table() -> Table:
    return LocalStack.create_table()


def receive_messages(queue: Queue):
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
