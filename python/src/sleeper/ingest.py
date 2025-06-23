import json
import uuid

from mypy_boto3_sqs import SQSServiceResource

from sleeper.properties.instance_properties import InstanceProperties, InstanceProperty


class IngestJob:
    def __init__(self, job_id: str = None, table_name: str = None, table_id: str = None, files: list[str] = None):
        if job_id is None:
            job_id = str(uuid.uuid4())
        if table_name is None and table_id is None:
            raise ValueError("Either table_name or table_id must be specified")
        if table_name is not None and table_id is not None:
            raise ValueError("Only one of table_name or table_id must be specified")
        if files is None:
            raise ValueError("files must be specified")
        self.job_id = job_id
        self.table_name = table_name
        self.table_id = table_id
        self.files = files

    def to_json(self) -> str:
        obj = {"id": self.job_id, "files": self.files}
        if self.table_name is not None:
            obj["tableName"] = self.table_name
        if self.table_id is not None:
            obj["tableId"] = self.table_id
        return json.dumps(obj)


class IngestJobSender:
    def __init__(self, sqs: SQSServiceResource, properties: InstanceProperties):
        self.sqs = sqs
        self.properties = properties

    def send(self, job: IngestJob, queue_url_property: InstanceProperty):
        queue_url = self.properties.get(queue_url_property)
        self.sqs.Queue(queue_url).send_message(MessageBody=job.to_json())
