from sleeper.properties.instance_properties import InstanceProperties
from sleeper.properties.user_defined_properties import CommonProperty
from sleeper.sleeper import SleeperClient
from tests.sleeper.localstack import LocalStack


class LocalStackSleeperClient:
    @staticmethod
    def create(properties: InstanceProperties) -> SleeperClient:
        return SleeperClient(
            instance_id=properties.get(CommonProperty.ID),
            s3_client=LocalStack.s3_client(),
            s3_resource=LocalStack.s3_resource(),
            s3_fs=LocalStack.s3fs(),
            sqs_resource=LocalStack.sqs_resource(),
            dynamo_resource=LocalStack.dynamo_resource(),
        )
