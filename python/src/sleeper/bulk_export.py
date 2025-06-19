import json
import uuid

from mypy_boto3_sqs import SQSServiceResource

from sleeper.properties.cdk_defined_properties import QueryCdkProperty
from sleeper.properties.instance_properties import InstanceProperties


class BulkExportQuery:
    def __init__(self, export_id: str = None, table_name: str = None, table_id: str = None):
        if export_id is None:
            export_id = str(uuid.uuid4())
        if table_name is None and table_id is None:
            raise ValueError("Either table_name or table_id must be specified")
        if table_name is not None and table_id is not None:
            raise ValueError("Only one of table_name or table_id must be specified")
        self.export_id = export_id
        self.table_name = table_name
        self.table_id = table_id

    def to_json(self) -> str:
        obj = {"exportId": self.export_id}
        if self.table_name is not None:
            obj["tableName"] = self.table_name
        if self.table_id is not None:
            obj["tableId"] = self.table_id
        return json.dumps(obj)


class BulkExportSender:
    def __init__(self, sqs: SQSServiceResource, properties: InstanceProperties):
        self.sqs = sqs
        self.properties = properties

    def send(self, query: BulkExportQuery):
        queue_url = self.properties.get(QueryCdkProperty.BULK_EXPORT_QUEUE_URL)
        self.sqs.Queue(queue_url).send_message(MessageBody=query.to_json())
