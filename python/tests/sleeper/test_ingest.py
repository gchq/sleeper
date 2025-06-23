import json

import pytest
from mypy_boto3_sqs.service_resource import Queue
from python.tests.sleeper.localstack import LocalStack

from sleeper.ingest import IngestJob, IngestJobSender
from sleeper.properties.cdk_defined_properties import IngestCdkProperty
from sleeper.properties.instance_properties import InstanceProperties
from tests.sleeper.properties.instance_properties_helper import create_test_instance_properties


def test_ingest(queue: Queue, sender: IngestJobSender):
    # Given
    job = IngestJob(job_id="test-job", table_name="test-table", files=["file-1.parquet"])

    # When
    sender.send(job)

    # Then
    assert [{"id": "test-job", "tableName": "test-table", "files": ["file-1.parquet"]}] == receive_messages(queue)


def test_ingest_by_table_id():
    # When
    job = IngestJob(job_id="test-job", table_id="test-table", files=["file-1.parquet"])

    # Then
    assert {"id": "test-job", "tableId": "test-table", "files": ["file-1.parquet"]} == json.loads(job.to_json())


def test_generate_job_id():
    # When
    job = IngestJob(table_name="test-table", files=["file-1.parquet"])

    # Then
    assert len(job.job_id) == 36


@pytest.fixture
def queue() -> Queue:
    return LocalStack.create_queue()


@pytest.fixture
def properties(queue: Queue) -> InstanceProperties:
    properties = create_test_instance_properties()
    properties.set(IngestCdkProperty.STANDARD_INGEST_QUEUE_URL, queue.url)
    return properties


@pytest.fixture
def sender(properties: InstanceProperties) -> IngestJobSender:
    return IngestJobSender(LocalStack.sqs_resource(), properties)


def receive_messages(queue: Queue):
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: json.loads(message.body), messages))
