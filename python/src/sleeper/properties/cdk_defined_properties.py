from sleeper.properties.instance_properties import BucketProperty, DynamoTableProperty, QueueProperty


class IngestQueue:
    STANDARD_INGEST = QueueProperty("sleeper.ingest.job.queue.url")
    BULK_IMPORT_EMR = QueueProperty("sleeper.bulk.import.emr.job.queue.url")
    BULK_IMPORT_PERSISTENT_EMR = QueueProperty("sleeper.bulk.import.persistent.emr.job.queue.url")
    BULK_IMPORT_EMR_SERVERLESS = QueueProperty("sleeper.bulk.import.emr.serverless.job.queue.url")
    BULK_IMPORT_EKS = QueueProperty("sleeper.bulk.import.eks.job.queue.url")


class QueryResources:
    QUERY_QUEUE = QueueProperty("sleeper.query.queue.url")
    QUERY_RESULTS_BUCKET = BucketProperty("sleeper.query.results.bucket")
    QUERY_TRACKER_TABLE = DynamoTableProperty("sleeper.query.tracker.table.name")
    BULK_EXPORT_QUEUE = QueueProperty("sleeper.bulk.export.queue.url")
