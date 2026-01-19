#  Copyright 2022-2025 Crown Copyright
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import json
import logging
import tempfile
import time
import uuid
from contextlib import contextmanager
from typing import Dict, List

import boto3
import s3fs
from boto3.dynamodb.conditions import Key
from mypy_boto3_dynamodb import DynamoDBServiceResource
from mypy_boto3_s3 import S3Client, S3ServiceResource
from mypy_boto3_sqs import SQSServiceResource
from pyarrow.parquet import ParquetFile

from pq.parquet_deserial import ParquetDeserialiser
from pq.parquet_serial import ParquetSerialiser
from sleeper.bulk_export import BulkExportQuery, BulkExportSender
from sleeper.ingest import IngestJob, IngestJobSender
from sleeper.ingest_batcher import IngestBatcherSender, IngestBatcherSubmitRequest
from sleeper.properties.cdk_defined_properties import CommonCdkProperty, IngestCdkProperty, QueryCdkProperty, queue_name_from_url
from sleeper.properties.config_bucket import load_instance_properties
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.query import Query, Region
from sleeper.web_socket_query import WebSocketQueryProcessor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Has a handler?
hasHandler: bool = len(logger.handlers) > 1
# Extract handler
handler: logging.Handler = logger.handlers[0] if hasHandler else logging.StreamHandler()
log_format_string: str = "%(asctime)s %(levelname)s %(filename)s/%(funcName)s %(message)s"
handler.setFormatter(logging.Formatter(log_format_string))
# Need to add it?
if not hasHandler:
    logger.addHandler(handler)
    logger.propagate = 0

DEFAULT_MAX_WAIT_TIME = 120
"""The default maximum amount of time to wait for queries to be finished."""


class SleeperClient:
    def __init__(
        self,
        instance_id,
        use_threads=False,
        s3_client: S3Client = None,
        s3_resource: S3ServiceResource = None,
        s3_fs: s3fs.S3FileSystem = None,
        sqs_resource: SQSServiceResource = None,
        dynamo_resource: DynamoDBServiceResource = None,
    ):
        if s3_client is None:
            s3_client = boto3.client("s3")
        if s3_resource is None:
            s3_resource = boto3.resource("s3")
        if s3_fs is None:
            s3_fs = s3fs.S3FileSystem(anon=False)
        if sqs_resource is None:
            sqs_resource = boto3.resource("sqs")
        if dynamo_resource is None:
            dynamo_resource = boto3.resource("dynamodb")
        self._instance_id = instance_id
        self._instance_properties = load_instance_properties(s3_resource, instance_id)
        self._s3_client = s3_client
        self._s3_resource = s3_resource
        self._s3_fs = s3_fs
        self._sqs_resource = sqs_resource
        self._dynamo_resource = dynamo_resource
        self._deserialiser = ParquetDeserialiser(use_threads=use_threads)

    def write_single_batch(self, table_name: str, rows_to_write: list, job_id: str = None):
        """
        Perform a write of the given rows to Sleeper.
        These are treated as a single block of rows to write. Each
        row should be a dictionary in the list.

        :param table_name: the table name to write to
        :param rows_to_write: list of the dictionaries containing the rows to write
        :param job_id: the id of the ingest job, will be randomly generated if not provided
        """
        # Generate a filename to write to
        databucket = self._instance_properties.get(CommonCdkProperty.DATA_BUCKET)
        databucket_file: str = f"{databucket}/{_make_ingest_object_key(job_id)}"
        logger.debug(f"Writing to {databucket_file}")
        # Upload it
        _write_and_upload_parquet(self._s3_client, rows_to_write, databucket_file)
        # Inform Sleeper
        job = IngestJob(job_id=job_id, table_name=table_name, files=[databucket_file])
        IngestJobSender(self._sqs_resource, self._instance_properties).send(job)

    def ingest_parquet_files_from_s3(self, table_name: str, files: list, job_id: str = None):
        """
        Ingests the data in the given files to the Sleeper table with name table_name. This is
        done by posting a message containing the list of files to the ingest queue. These
        files must be in S3. They can be either files or directories. If they are directories
        then all Parquet files under the directory will be ingested. Files should be specified
        in the format 'bucket/file'.

        :param table_name: the table name to write to
        :param files: list of the files containing the rows to ingest
        :param job_id: the id of the ingest job, will be randomly generated if not provided
        """
        job = IngestJob(job_id=job_id, table_name=table_name, files=files)
        IngestJobSender(self._sqs_resource, self._instance_properties).send(job)

    def bulk_import_parquet_files_from_s3(
        self,
        table_name: str,
        files: list,
        id: str = None,
        platform: str = "EMR",
        platform_spec: dict = None,
        class_name: str = None,
    ):
        """
        Ingests the data in the given files to the Sleeper table with name table_name using the bulk
        import method. This is done by posting a message containing the list of files to the bulk
        import queue. These files must be in S3. They can be either files or directories. If they
        are directories then all Parquet files under the directory will be ingested. Files should
        be specified in the format 'bucket/file'.

        :param table_name: the table name to write to
        :param files: list of the files containing the rows to ingest
        :param id: the id of the bulk import job - if one is not provided then a UUID will be assigned
        :param platform: the platform to use - either "EMR" or "PersistentEMR" or "EKS"
        :param platform_spec: a dict containing details of the platform to use - see docs/usage/python-api.md
        """
        _bulk_import(
            self._sqs_resource,
            table_name,
            files,
            self._instance_properties,
            id,
            platform,
            platform_spec,
            class_name,
        )

    def submit_to_ingest_batcher(self, table_name: str, files: list[str]):
        """
        Submits files to the ingest batcher to be added to a Sleeper table. This request is submitted to an SQS queue
        and processed asynchronously. The files will be tracked in the batcher and then ingested to the table later
        based on the configuration of the batcher.

        These files must be in S3. They can be either files or directories. If they are directories then all Parquet
        files under the directory will be ingested. Files should be specified in the format 'bucket/file'.

        :param table_name: the table name to write to
        :param files: list of the files containing the rows to ingest
        """

        request = IngestBatcherSubmitRequest(table_name=table_name, files=files)
        IngestBatcherSender(self._sqs_resource, self._instance_properties).send(request)

    def bulk_export(self, query: BulkExportQuery):
        """
        Exports data from a Sleeper table. This is submitted to an SQS queue and processed asynchronously. Data will be
        written to the bulk export results S3 bucket.

        :param query: the bulk export query to send
        """
        BulkExportSender(self._sqs_resource, self._instance_properties).send(query)

    async def web_socket_exact_key_query(self, table_name: str, keys: dict, query_id: str = str(uuid.uuid4()), strings_base64_encoded: bool = False) -> list:
        """
        Asynchronously performs a web socket query on a Sleeper table for rows where the key matches a given list of query keys.
        The results are aggregated into a single list.

        :param table_name: The name of the table to query.
        :param keys: A dictionary where each key is a column name and each value is a list of values to query for.
                     Example: {"key":["a", "z"]}
        :param query_id: An optional query ID; defaults to a new UUID if not provided.
        :param strings_base64_encoded: Boolean indicating if string values are base64 encoded.

        :return: A list containing all retrieved rows matching the specified keys and values.
        """
        query = Query(query_id, table_name, Region.list_from_field_to_exact_values(keys, strings_base64_encoded))
        return await self.query_by_web_socket(query)

    async def web_socket_range_key_query(
        self, table_name: str, keys: list, query_id: str = str(uuid.uuid4()), min_inclusive: bool = True, max_inclusive: bool = True, strings_base64_encoded: bool = False
    ) -> list:
        """
        Asynchronously performs a web socket query on a Sleeper table for rows where the key is within specified ranges.
        The results are aggregated into a single list.

        :param table_name: The name of the table to query.
        :param keys: A list of dictionaries, each mapping a key to a range dict with 'min' and 'max' keys.
                     Example: [{'key1': {'min': 1, 'max': 10}}, {'key2': {'min': 5, 'max': 15}}]
        :param query_id: An optional query ID; defaults to a new UUID if not provided.
        :param min_inclusive: Boolean indicating if the minimum boundary is inclusive.
        :param max_inclusive: Boolean indicating if the maximum boundary is inclusive.
        :param strings_base64_encoded: Boolean indicating if string values are base64 encoded.

        :return: A list containing all retrieved rows matching the specified key ranges.
        """
        query = Query(query_id, table_name, [Region.from_field_to_dict(region, min_inclusive, max_inclusive, strings_base64_encoded) for region in keys])
        return await self.query_by_web_socket(query)

    def exact_key_query(self, table_name: str, keys, query_id: str = None) -> list:
        """
        Query a Sleeper table for rows where the key values are equal to one of a given list. This query is executed in
        a lambda function and the results are written to S3. Once the query has finished the results are loaded from
        S3. This means that there can be significant latency before the results are returned. Note that the first query
        will be significantly slower than subsequent ones as the lambda needs to start up, unless the KeepLambdaWarm
        stack is deployed.

        :param table_name: the table to query
        :param keys: either a single dict where the key is the row-key field name and the value is a list of values
        to query for, or a list of dicts where the key is a row-key field name and the value is the value to query for
        :param query_id: the query ID, will be randomly generated if not provided

        :return: list of result rows
        """
        if isinstance(keys, dict):
            regions = Region.list_from_field_to_exact_values(keys)
        elif isinstance(keys, list):
            regions = [Region.from_field_to_exact_value(region) for region in keys]
        else:
            raise Exception(
                "keys must be either (a) a single dict where the key is the row-key field name and the value is a list of values to query for"
                + " or (b) a list of dicts where the key is a row-key field name and the value is the value to query for"
            )

        return self.query_by_sqs(Query(query_id, table_name, regions))

    def range_key_query(self, table_name: str, regions: list, query_id: str = None) -> list:
        """
        Query a Sleeper table for rows where the key values are within one of a list of regions. This query is
        executed in a lambda function and the results are written to S3. Once the query has finished the results are
        loaded from S3. This means that there can be significant latency before the results are returned. Note that the
        first query will be significantly slower than subsequent ones as the lambda needs to start up, unless the
        KeepLambdaWarm stack is deployed.

        :param table_name: the table to query
        :param regions: a list of regions; each region should be a dictionary where the key is a row key field name
            and the value is the range for that field, given as a tuple of length either 2 or 4. If length 2
            then the first element is the min of the range and the second is the max of the range (use None for no
            maximum). If length 4 then the first element is the min of the range, the next is a boolean specifying
            whether the minimum is inclusive, the third is the max of the range and the next is a boolean specifying
            whether the maximum is inclusive.
        :param query_id: the query ID, will be randomly generated if not provided

        :return: list of the result rows
        """
        return self.query_by_sqs(Query(query_id=query_id, table_name=table_name, regions=[Region.from_field_to_tuple(region) for region in regions]))

    def query_by_sqs(self, query: Query):
        """
        Query a Sleeper table for rows where the key values are within one of a list of regions. This query is
        executed in a lambda function and the results are written to S3. Once the query has finished the results are
        loaded from S3. This means that there can be significant latency before the results are returned. Note that the
        first query will be significantly slower than subsequent ones as the lambda needs to start up, unless the
        KeepLambdaWarm stack is deployed.

        :param query: the query

        :return: list of the result rows
        """
        message = query.to_dict()
        logger.debug(message)
        message_json = json.dumps(message)

        query_queue_name = queue_name_from_url(self._instance_properties.get(QueryCdkProperty.QUERY_QUEUE_URL))
        query_queue_sqs = self._sqs_resource.get_queue_by_name(QueueName=query_queue_name)
        query_queue_sqs.send_message(MessageBody=message_json)

        logger.debug(f"Submitted query with id {query.query_id}")

        located_rows = _receive_messages(self._instance_properties, self._s3_resource, self._s3_fs, self._dynamo_resource, self._deserialiser, query.query_id)
        return located_rows

    async def query_by_web_socket(self, query: Query):
        """
        Asynchronously queries a Sleeper table over a web socket for rows where the key is within specified ranges.
        The results are aggregated into a single list.

        :param query: the query

        :return: list of the result rows
        """
        return await WebSocketQueryProcessor(instance_properties=self._instance_properties).process_query(query)

    @contextmanager
    def create_batch_writer(self, table_name: str, job_id: str = None):
        """
        Creates an object for writing large batches of events to Sleeper.
        Designed to be used within a context manager ('with' statement).
        When this is used, a temporary Parquet file will be created locally
        so that events can be written to it as necessary. Once it is closed,
        the Parquet file of rows will be uploaded to S3 and ingested into
        Sleeper.

        The main difference between this method and write_ingest is that rows
        do not have to be completely held in memory and passed in one function call,
        data can be sent in batches as it is created.

        See the examples for how to use this method.

        :param table_name: the table to ingest to
        :param job_id: the id of the ingest job, will be randomly generated if not provided
        :return: an object for use with context managers
        """
        # Create a temporary file to write data to as it is batched
        with tempfile.NamedTemporaryFile() as fp:
            try:
                parquet_file = ParquetSerialiser(fp)

                # Return an instance of RowWriter that is bound to this batch writer

                class RowWriter:
                    def __init__(self):
                        self.num_rows: int = 0

                    def write(self, rows: List[Dict]):
                        for row in rows:
                            parquet_file.write_record(row)

                        # Keep num rows updated
                        self.num_rows += len(rows)

                writer = RowWriter()

                # Return this as the context manager object
                yield writer

            finally:
                # Now we can close up the Parquet file and start the Sleeper ingest
                parquet_file.write_tail()

                # Get name of file to upload to on S3
                bucket = self._instance_properties.get(CommonCdkProperty.DATA_BUCKET)
                key: str = _make_ingest_object_key(job_id)

                # Perform upload
                self._s3_client.upload_file(fp.name, bucket, key)
                logger.debug(f"Uploaded {writer.num_rows} rows to S3")

                # Notify Sleeper
                job = IngestJob(job_id=job_id, table_name=table_name, files=[f"{bucket}/{key}"])
                IngestJobSender(self._sqs_resource, self._instance_properties).send(job)


def _write_and_upload_parquet(s3_client: S3Client, rows_to_write: list, s3_file: str):
    """
    Creates a Parquet file from the user's rows and uploads to their Sleeper databucket.

    :param rows_to_write: list of the dictionaries containing the rows to user wants to write to the Parquet file

    :param s3_file: name of the databucket concatenated with the name of the Parquet file
    """
    # Separates the argument into the databucket name and the filename they want to appear in the bucket
    databucket_name = s3_file.split("/", 1)[0]
    file_name = s3_file.split("/", 1)[1]

    with tempfile.NamedTemporaryFile() as fp:
        writer = ParquetSerialiser(fp)
        for row in rows_to_write:
            writer.write_record(row)
        writer.write_tail()

        s3_client.upload_file(fp.name, databucket_name, file_name)


def _bulk_import(
    sqs: SQSServiceResource,
    table_name: str,
    files_to_ingest: list,
    instance_properties: InstanceProperties,
    job_id: str,
    platform: str,
    platform_spec: dict,
    class_name: str,
):
    """
    Instructs Sleeper to bulk import the given files from S3.

    :param table_name: table name to bulk import to
    :param files_to_ingest: path to the file on the S3 databucket which is to be bulk imported
    :param emr_bulk_import_queue: name of the Sleeper instance's non-persistent EMR bulk import queue
    :param persistent_emr_bulk_import_queue: name of the Sleeper instance's persistent EMR bulk import queue
    :param eks_bulk_import_queue: name of the Sleeper instance's EKS bulk import queue
    :param job_id: the id of the bulk import job, will be randomly generated if not provided
    :param platform: the platform to use - either "EMR", "PersistentEMR", "EKS", or "EMRServerless"
    :param platform_spec: a dict containing details of the platform to use - see docs/usage/python-api.md
    """
    if platform != "EMR" and platform != "EKS" and platform != "PersistentEMR" and platform != "EMRServerless":
        raise Exception("Platform must be 'EMR' or 'PersistentEMR' or 'EKS' or 'EMRServerless'")

    if platform == "EMR":
        queue_property = IngestCdkProperty.BULK_IMPORT_EMR_QUEUE_URL
    elif platform == "PersistentEMR":
        queue_property = IngestCdkProperty.BULK_IMPORT_PERSISTENT_EMR_QUEUE_URL
    elif platform == "EKS":
        queue_property = IngestCdkProperty.BULK_IMPORT_EKS_QUEUE_URL
    else:
        queue_property = IngestCdkProperty.BULK_IMPORT_EMR_SERVERLESS_QUEUE_URL
    if job_id is None:
        job_id = str(uuid.uuid4())

    queue = queue_name_from_url(instance_properties.get(queue_property))

    # Creates the ingest message and generates and ID
    bulk_import_message = {
        "id": job_id,
        "tableName": table_name,
        "files": files_to_ingest,
    }
    if platform_spec is not None:
        bulk_import_message["platformSpec"] = platform_spec
    if class_name is not None:
        bulk_import_message["className"] = class_name

    # Converts bulk import message to json and sends to the SQS queue
    bulk_import_message_json = json.dumps(bulk_import_message)
    logger.debug(f"Sending JSON message to {platform} queue: {bulk_import_message_json}")
    bulk_import_queue_sqs = sqs.get_queue_by_name(QueueName=queue)
    bulk_import_queue_sqs.send_message(MessageBody=bulk_import_message_json)


def _receive_messages(
    instance_properties: InstanceProperties,
    s3: S3ServiceResource,
    s3fs: s3fs.S3FileSystem,
    dynamo: DynamoDBServiceResource,
    deserialiser: ParquetDeserialiser,
    query_id: str,
    timeout: int = DEFAULT_MAX_WAIT_TIME,
) -> List:
    """
    Polls the DynamoDB query tracker until the query is completed, then reads the results from S3.

    :param query_id: the id of the Sleeper query
    :param timeout: the maximum time in seconds to wait for a result

    :return: a list of the rows the user queried for
    """
    # This while loop will poll the DynamoDB query tracker until the query is completed. Upon completion the
    # results will be read from S3 into a list and returned.
    results_table = dynamo.Table(instance_properties.get(QueryCdkProperty.QUERY_TRACKER_TABLE))

    timer = time.time()
    end_time = timeout + timer
    count = 0
    while time.time() < end_time:
        if count < 10:
            sleep_time = 0.5
        elif count < 35:
            sleep_time = 2
        else:
            sleep_time = 5
        count += 1
        logger.debug(f"Sleeping for {sleep_time} seconds before polling DynamoDB query results tracker")
        time.sleep(sleep_time)

        # Get current status of query
        query_status_response = results_table.query(KeyConditionExpression=Key("queryId").eq(query_id) & Key("subQueryId").eq("-"))

        if len(query_status_response["Items"]) == 0:
            continue

        lastKnownState = query_status_response["Items"][0]["lastKnownState"]

        if lastKnownState == "FAILED":
            logger.debug("Query failed")
            raise RuntimeError("Query failed")
        elif lastKnownState == "COMPLETED":
            # Query results are put as Parquet files in the results bucket under "query-<query id>/"
            # (see Java class S3ResultsOutput for the precise formation of the location of the result files)
            results_files = []
            results_bucket_name = instance_properties.get(QueryCdkProperty.QUERY_RESULTS_BUCKET)
            for object_summary in s3.Bucket(results_bucket_name).objects.filter(Prefix=f"query-{query_id}"):
                logger.debug(f"Found {object_summary.key}")
                if object_summary.key.endswith(".parquet"):
                    results_files.append(object_summary.key)

            results = []
            for file in results_files:
                logger.debug(f"Opening file {results_bucket_name}/{file}")
                with s3fs.open(f"{results_bucket_name}/{file}", "rb") as f:
                    with ParquetFile(f) as po:
                        for row in deserialiser.read(po):
                            results.append(row)

            logger.debug("Query has finished")
            return results
    raise RuntimeError("No results received from Sleeper within specified timeout.")


def _make_ingest_object_key(id: str = None) -> str:
    if id is None:
        id = str(uuid.uuid4())
    return f"for_ingest/{id}.parquet"
