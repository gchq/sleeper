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

    def exact_key_query(self, table_name: str, keys, query_id: str = None) -> list:
        """
        Query a Sleeper table for rows where the key matches a given list of query keys. This query is executed in
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
        if not isinstance(keys, list) and not isinstance(keys, dict):
            raise Exception(
                "keys must be either (a) a single dict where the key is the row-key field name and the value is a list of values to query for"
                + " or (b) a list of dicts where the key is a row-key field name and the value is the value to query for"
            )
        if len(keys) == 0:
            raise Exception("Must provide at least one key")

        if isinstance(keys, dict):
            if len(keys) != 1:
                raise Exception("If keys is a dict, there must be only one entry, with key of the row-key field and the value a list of the values to query for")
            for key in keys:
                return self._exact_key_query_from_list_of_values(table_name, key, keys[key], query_id)

        return self._exact_key_query_from_dicts(table_name, keys, query_id)

    def _exact_key_query_from_list_of_values(self, table_name: str, row_key_field_name: str, values: list, query_id: str) -> list:
        regions = []
        for value in values:
            region = {row_key_field_name: [value, True, value, True]}
            regions.append(region)
        return self.range_key_query(table_name, regions, query_id)

    def _exact_key_query_from_dicts(self, table_name: str, keys: dict, query_id: str) -> list:
        regions = []
        for key in keys:
            if not isinstance(key, dict):
                raise Exception("Each key must be a dict mapping row-key field name to value")
            if len(key) == 0:
                raise Exception("Key must not be empty")
            region = {}
            for field_name in key:
                region[field_name] = [key[field_name], True, key[field_name], True]
            regions.append(region)
        return self.range_key_query(table_name, regions, query_id)

    def range_key_query(self, table_name: str, regions: list, query_id: str = None) -> list:
        """
        Query a Sleeper table for rows where the key is within one of the provided list of ranges. This query is
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
        if query_id is None:
            query_id = str(uuid.uuid4())
        json_regions_list = []
        if not isinstance(regions, list):
            raise Exception("Regions must be a list")
        if len(regions) == 0:
            raise Exception("Must provide at least one region")
        for region in regions:
            if not isinstance(region, dict):
                raise Exception("Each region must be a dict mapping row-key field name to range")
            if len(region) == 0:
                raise Exception("Region must not be empty")
            json_region = {}
            for field_name in region:
                range_as_tuple = region[field_name]
                if not len(range_as_tuple) == 2 and not len(range_as_tuple) == 4:
                    raise Exception("Each range must be of length 2 (min, max) or 4 (min, minInclusive, max, maxInclusive)")
                if len(range_as_tuple) == 2:
                    min = range_as_tuple[0]
                    max = range_as_tuple[1]
                    min_inclusive = True
                    max_inclusive = False
                else:
                    min = range_as_tuple[0]
                    # TODO Check type
                    min_inclusive = range_as_tuple[1]
                    max = range_as_tuple[2]
                    max_inclusive = range_as_tuple[3]
                json_range = {
                    "min": min,
                    "minInclusive": min_inclusive,
                    "max": max,
                    "maxInclusive": max_inclusive,
                }
                json_region[field_name] = json_range
                json_region["stringsBase64Encoded"] = False
            json_regions_list.append(json_region)

        query_message = {
            "queryId": query_id,
            "tableName": table_name,
            "type": "Query",
            "regions": json_regions_list,
        }

        logger.debug(query_message)

        # Convert query message to json and send to query queue
        query_message_json = json.dumps(query_message)
        query_queue_name = queue_name_from_url(self._instance_properties.get(QueryCdkProperty.QUERY_QUEUE_URL))
        query_queue_sqs = self._sqs_resource.get_queue_by_name(QueueName=query_queue_name)
        query_queue_sqs.send_message(MessageBody=query_message_json)

        logger.debug(f"Submitted query with id {query_id}")

        located_rows = _receive_messages(self._instance_properties, self._s3_resource, self._s3_fs, self._dynamo_resource, self._deserialiser, query_id)
        return located_rows

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
    if platform_spec is not None and platform != "PersistentEMR":
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
