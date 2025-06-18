from sleeper.bulk_export import BulkExportQuery, BulkExportSender
from sleeper.properties.cdk_defined_properties import QueryResources
from sleeper.properties.instance_properties import InstanceProperties
from tests.sleeper.localstack import LocalStack

properties = InstanceProperties()
queue = LocalStack.create_queue()
properties.set(QueryResources.BULK_EXPORT_QUEUE, queue.url)


def test_bulk_export():
    # Given
    query = BulkExportQuery(export_id="test-export", table_name="test-table")

    # When
    sender().send(query)

    # Then
    assert [""] == receive_messages()


def sender():
    return BulkExportSender(LocalStack.sqs_resource(), properties)


def receive_messages():
    messages = queue.receive_messages(WaitTimeSeconds=0)
    return list(map(lambda message: message.body, messages))
