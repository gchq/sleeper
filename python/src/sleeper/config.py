
import configparser
from mypy_boto3_s3.service_resource import S3ServiceResource

def load_instance_properties_from_bucket(bucket_name: str, s3_resource: S3ServiceResource) -> dict:
    config_obj = s3_resource.Object(bucket_name, 'instance.properties')
    config_str = config_obj.get()['Body'].read().decode('utf-8')
    config_str = '[asection]\n' + config_str
    config = configparser.ConfigParser(allow_no_value=True)
    config.read_string(config_str)
    return dict(config['asection'])
