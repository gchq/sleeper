import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from sleeper.exceptions import SleeperApiError, SleeperConfigurationError
from sleeper.properties import CommonProperty, RestCdkProperty
from sleeper.rest import RestApiClient
from sleeper.rest.table import AddTableResponse, TableSchema
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties

ENDPOINT = "http://testing.api.aws"


def should_throw_exception_when_stack_not_deployed():
    instance_properties = create_test_instance_properties()

    with pytest.raises(SleeperConfigurationError, match="REST API stack has not been deployed"):
        RestApiClient(instance_properties=instance_properties)


@patch("sleeper.rest.rest_client.requests.post")
def should_test_client_creates_table(mock_post, rest_client: RestApiClient):
    mock_post.return_value.status_code = 201
    mock_post.return_value.json.return_value = {
        "tableName": "testing",
        "tableId": "123",
    }

    schema = TableSchema(
        rowKeyFields=[],
        sortKeyFields=[],
        valueFields=[],
    )

    response: AddTableResponse = rest_client.add_table(
        table_name="testing",
        schema=schema,
        split_points=None,
    )

    mock_post.assert_called_once()

    args, kwargs = mock_post.call_args

    assert args[0] == f"{ENDPOINT}/{CommonProperty.ADD_TABLE_PATH}"
    assert kwargs["timeout"] == 30

    payload = json.loads(kwargs["data"])
    assert payload["properties"]["sleeper.table.name"] == "testing"
    assert payload["schema"] == {
        "rowKeyFields": [],
        "sortKeyFields": [],
        "valueFields": [],
    }
    assert payload["splitPoints"] is None

    auth_header = kwargs["headers"]["Authorization"]

    assert "SignedHeaders=" in auth_header
    assert "Signature=" in auth_header

    assert response.tableName == "testing"
    assert response.tableId == "123"


@patch("sleeper.rest.rest_client.requests.post")
def should_test_client_tries_to_create_existing_table(mock_post, rest_client: RestApiClient):
    error_response = MagicMock()
    error_response.status_code = 409
    error_response.json.return_value = {"message": "Table already exists", "error": "table_already_exists"}

    http_error = requests.HTTPError("409 Client Error: Conflict")
    http_error.response = error_response

    mock_post.return_value.raise_for_status.side_effect = http_error

    schema = TableSchema(
        rowKeyFields=[],
        sortKeyFields=[],
        valueFields=[],
    )

    with pytest.raises(SleeperApiError) as exc_info:
        rest_client.add_table(
            table_name="testing",
            schema=schema,
            split_points=None,
        )
        assert "Table already exists" in str(exc_info.value)


@pytest.fixture
def rest_client() -> RestApiClient:
    properties = create_test_instance_properties()
    properties.set(RestCdkProperty.REST_BASE_URL, ENDPOINT)
    client = RestApiClient(instance_properties=properties)
    return client


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "test")
