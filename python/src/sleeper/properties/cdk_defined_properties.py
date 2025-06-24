from sleeper.properties.instance_properties import InstanceProperty


class IngestCdkProperty:
    STANDARD_INGEST_QUEUE_URL = InstanceProperty("sleeper.ingest.job.queue.url")
    BULK_IMPORT_EMR_QUEUE_URL = InstanceProperty("sleeper.bulk.import.emr.job.queue.url")
    BULK_IMPORT_PERSISTENT_EMR_QUEUE_URL = InstanceProperty("sleeper.bulk.import.persistent.emr.job.queue.url")
    BULK_IMPORT_EMR_SERVERLESS_QUEUE_URL = InstanceProperty("sleeper.bulk.import.emr.serverless.job.queue.url")
    BULK_IMPORT_EKS_QUEUE_URL = InstanceProperty("sleeper.bulk.import.eks.job.queue.url")
    INGEST_BATCHER_SUBMIT_QUEUE_URL = InstanceProperty("sleeper.ingest.batcher.submit.queue.url")


class QueryCdkProperty:
    QUERY_QUEUE_URL = InstanceProperty("sleeper.query.queue.url")
    QUERY_RESULTS_BUCKET = InstanceProperty("sleeper.query.results.bucket")
    QUERY_TRACKER_TABLE = InstanceProperty("sleeper.query.tracker.table.name")
    BULK_EXPORT_QUEUE_URL = InstanceProperty("sleeper.bulk.export.queue.url")


class CommonCdkProperty:
    CONFIG_BUCKET = InstanceProperty("sleeper.config.bucket")
    DATA_BUCKET = InstanceProperty("sleeper.data.bucket")


def queue_name_from_url(queue_url: str) -> str:
    return queue_url.rsplit("/", 1)[1]
