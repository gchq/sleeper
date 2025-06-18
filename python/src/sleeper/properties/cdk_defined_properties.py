
from sleeper.properties.bucket_property import BucketProperty
from sleeper.properties.dynamo_table_property import DynamoTableProperty
from sleeper.properties.queue_property import QueueProperty


class IngestQueue:

    STANDARD_INGEST = QueueProperty('sleeper.ingest.job.queue.url')
    BULK_IMPORT_EMR = QueueProperty('sleeper.bulk.import.emr.job.queue.url')
    BULK_IMPORT_PERSISTENT_EMR = QueueProperty('sleeper.bulk.import.persistent.emr.job.queue.url')
    BULK_IMPORT_EMR_SERVERLESS = QueueProperty('sleeper.bulk.import.emr.serverless.job.queue.url')
    BULK_IMPORT_EKS = QueueProperty('sleeper.bulk.import.eks.job.queue.url')

class QueryResources:

    QUERY_QUEUE = QueueProperty('sleeper.query.queue.url')
    QUERY_RESULTS_BUCKET = BucketProperty('sleeper.query.results.bucket')
    QUERY_TRACKER_TABLE = DynamoTableProperty('sleeper.query.tracker.table.name')
