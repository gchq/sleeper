#  Copyright 2022-2023 Crown Copyright
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
import configparser
import json
import logging
import tempfile
import time
import uuid
from contextlib import contextmanager
from typing import Dict, List, Tuple

import boto3
import s3fs
from boto3.dynamodb.conditions import Key
from pq.parquet_deserial import ParquetDeserialiser
from pq.parquet_serial import ParquetSerialiser

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Has a handler?
hasHandler: bool = len(logger.handlers) > 1
# Extract handler
handler: logging.Handler = logger.handlers[0] if hasHandler else logging.StreamHandler()
log_format_string: str = '%(asctime)s %(levelname)s %(filename)s/%(funcName)s %(message)s'
handler.setFormatter(logging.Formatter(log_format_string))
# Need to add it?
if not hasHandler:
    logger.addHandler(handler)
    logger.propagate = 0

_s3 = boto3.client('s3')
_s3_resource = boto3.resource('s3')
_sqs = boto3.resource('sqs')
_dynanmodb = boto3.resource('dynamodb')

DEFAULT_MAX_WAIT_TIME = 120
"""The default maximum amount of time to wait for queries to be finished."""


class SleeperClient:

    def __init__(self, basename):
        self._basename = basename
        resources = _get_resource_names("sleeper-" + self._basename + "-config")
        self._ingest_queue = resources[0]
        self._emr_bulk_import_queue = resources[1]
        self._persistent_emr_bulk_import_queue = resources[2]
        self._eks_bulk_import_queue = resources[3]
        self._query_queue = resources[4]
        self._query_results_bucket = resources[5]
        self._dynamodb_query_tracker_table = resources[6]
        logger.debug("Loaded properties from config bucket sleeper-" + self._basename + "-config")
        self._s3fs = s3fs.S3FileSystem(anon=False)  # uses default credentials

    def write_single_batch(self, table_name: str, records_to_write: list, job_id: str = None):
        """
        Perform a write of the given records to Sleeper.
        These are treated as a single block of records to write. Each
        record should be a dictionary in the list.

        :param table_name: the table name to write to
        :param records_to_write: list of the dictionaries containing the records to write
        :param job_id: the ingest job ID to use, will be randomly generated if not provided 
        """
        # Generate a filename to write to
        databucket: str = _make_ingest_bucket_name(
            self._basename, table_name)
        databucket_file = _make_ingest_s3_name(databucket)
        logger.debug(f"Writing to {databucket_file}")
        # Upload it
        _write_and_upload_parquet(records_to_write, databucket_file)
        # Inform Sleeper
        _ingest(table_name, [databucket_file], self._ingest_queue, job_id)

    def ingest_parquet_files_from_s3(self, table_name: str, files: list, job_id: str = None):
        """
        Ingests the data in the given files to the Sleeper table with name table_name. This is
        done by posting a message containing the list of files to the ingest queue. These
        files must be in S3. They can be either files or directories. If they are directories
        then all Parquet files under the directory will be ingested. Files should be specified
        in the format 'bucket/file'.

        :param table_name: the table name to write to
        :param files: list of the files containing the records to ingest
        :param job_id: the ingest job ID to use, will be randomly generated if not provided 
        """
        _ingest(table_name, files, self._ingest_queue, job_id)

    def bulk_import_parquet_files_from_s3(self, table_name: str, files: list,
                                          id: str = None, platform: str = "EMR", platform_spec: dict = None,
                                          class_name: str = None):
        """
        Ingests the data in the given files to the Sleeper table with name table_name using the bulk
        import method. This is done by posting a message containing the list of files to the bulk
        import queue. These files must be in S3. They can be either files or directories. If they
        are directories then all Parquet files under the directory will be ingested. Files should
        be specified in the format 'bucket/file'.

        :param table_name: the table name to write to
        :param files: list of the files containing the records to ingest
        :param id: the id of the bulk import job - if one is not provided then a UUID will be assigned
        :param platform: the platform to use - either "EMR" or "PersistentEMR" or "EKS"
        :param platform_spec: a dict containing details of the platform to use - see docs/python-api.md
        """
        _bulk_import(table_name, files, self._emr_bulk_import_queue, self._persistent_emr_bulk_import_queue,
                     self._eks_bulk_import_queue, id, platform, platform_spec, class_name)

    def exact_key_query(self, table_name: str, keys) -> list:
        """
        Query a Sleeper table for records where the key matches a given list of query keys.

        :param table_name: the table to query
        :param keys: either a dict with key the row-key field name and value a list of values to query for, or a list of dicts where the key is a row-key field name and the value is the value

        :return: list of result records
        """
        if not isinstance(keys, list) and not isinstance(keys, dict):
            raise Exception(
                "keys must be either (a) a single dict where the key is the row-key field name and the value is a list of values to query for"
                + " or (b) a list of dicts where the key is a row-key field name and the value is the value to query for")
        if len(keys) == 0:
            raise Exception("Must provide at least one key")

        if isinstance(keys, dict):
            if len(keys) != 1:
                raise Exception(
                    "If keys is a dict, there must be only one entry, with key of the row-key field and the value a list of the values to query for")
            for key in keys:
                return self._exact_key_query_from_list_of_values(table_name, key, keys[key])

        return self._exact_key_query_from_dicts(table_name, keys)

    def _exact_key_query_from_list_of_values(self, table_name: str, row_key_field_name: str, values: list) -> list:
        regions = []
        for value in values:
            region = {row_key_field_name: [value, True, value, True]}
            regions.append(region)
        return self.range_key_query(table_name, regions)

    def _exact_key_query_from_dicts(self, table_name: str, keys: list) -> list:
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
        return self.range_key_query(table_name, regions)

    def range_key_query(self, table_name: str, regions: list) -> list:
        """
        Query a Sleeper table for records where the key is within one of the provided list of ranges.

        :param table_name: the table to query
        :param regions: a list of regions; each region should be a dictionary where the key is a row key field name
            and the value is the range for that field, given as a tuple of length either 2 or 4. If length 2
            then the first element is the min of the range and the second is the max of the range (use None for no
            maximum). If length 4 then the first element is the min of the range, the next is a boolean specifying
            whether the minimum is inclusive, the third is the max of the range and the next is a boolean specifying
            whether the maximum is inclusive.

        :return: list of the result records
        """
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
                    raise Exception(
                        "Each range must be of length 2 (min, max) or 4 (min, minInclusive, max, maxInclusive)")
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
                    "maxInclusive": max_inclusive
                }
                json_region[field_name] = json_range
                json_region["stringsBase64Encoded"] = False
            json_regions_list.append(json_region)

        query_id = str(uuid.uuid4())
        query_message = {
            'queryId': query_id,
            'tableName': table_name,
            'type': 'Query',
            'regions': json_regions_list
        }

        print(query_message)

        # Convert query message to json and send to query queue
        query_message_json = json.dumps(query_message)
        query_queue_sqs = _sqs.get_queue_by_name(QueueName=self._query_queue)
        query_queue_sqs.send_message(MessageBody=query_message_json)

        logger.debug(f"Submitted query with id {query_id}")

        located_records = _receive_messages(self, query_id)
        return located_records

    @contextmanager
    def create_batch_writer(self, table_name: str):
        """
        Creates an object for writing large batches of events to Sleeper.
        Designed to be used within a context manager ('with' statement).
        When this is used, a temporary Parquet file will be created locally
        so that events can be written to it as necessary. Once it is closed,
        the Parquet file of records will be uploaded to S3 and ingested into
        Sleeper.

        The main difference between this method and write_ingest is that records
        do not have to be completely held in memory and passed in one function call,
        data can be sent in batches as it is created.

        See the examples for how to use this method.

        :param table_name: the table to ingest to
        :return: an object for use with context managers
        """
        # Create a temporary file to write data to as it is batched
        with tempfile.NamedTemporaryFile() as fp:
            try:
                parquet_file = ParquetSerialiser(fp)

                # Return an instance of RecordWriter that is bound to this batch writer

                class RecordWriter:
                    def __init__(self):
                        self.num_records: int = 0

                    def write(self, records: List[Dict]):
                        for record in records:
                            parquet_file.write_record(record)

                        # Keep num records updated
                        self.num_records += len(records)

                writer = RecordWriter()

                # Return this as the context manager object
                yield writer

            finally:
                # Now we can close up the Parquet file and start the Sleeper ingest
                parquet_file.write_tail()

                # Get name of file to upload to on S3
                databucket: str = _make_ingest_bucket_name(
                    self._basename, table_name)
                s3_filename: str = _make_ingest_s3_name(databucket)
                bucket: str = s3_filename.split('/', 1)[0]
                key: str = s3_filename.split('/', 1)[1]

                # Perform upload
                _s3.upload_file(fp.name, bucket, key)
                logger.debug(f"Uploaded {writer.num_records} records to S3")

                # Notify Sleeper
                _ingest(table_name, [s3_filename], self._ingest_queue)


def _get_resource_names(configbucket: str) -> Tuple[str, str, str, str, str, str, str]:
    """
    Gets SQS queue names from S3 for the posting of messages to Sleeper.

    :param configbucket: name of the S3 bucket where the config is

    :return: tuple with the names of the queues
    """
    config_obj = _s3_resource.Object(configbucket, 'config')
    config_str = config_obj.get()['Body'].read().decode('utf-8')
    config_str = '[asection]\n' + config_str
    config = configparser.ConfigParser(allow_no_value=True)
    config.read_string(config_str)

    # The string keys for these properties can be found in the Java class SystemDefinedInstanceProperty
    ingest_queue = None
    if 'sleeper.ingest.job.queue.url' in config['asection']:
        ingest_queue = (config['asection']['sleeper.ingest.job.queue.url']).rsplit('/', 1)[1]

    emr_bulk_import_queue = None
    if 'sleeper.bulk.import.emr.job.queue.url' in config['asection']:
        emr_bulk_import_queue = (config['asection']['sleeper.bulk.import.emr.job.queue.url']).rsplit('/', 1)[1]

    persistent_emr_bulk_import_queue = None
    if 'sleeper.bulk.import.persistent.emr.job.queue.url' in config['asection']:
        persistent_emr_bulk_import_queue = \
            (config['asection']['sleeper.bulk.import.persistent.emr.job.queue.url']).rsplit('/', 1)[1]

    eks_bulk_import_queue = None
    if 'sleeper.bulk.import.eks.job.queue.url' in config['asection']:
        eks_bulk_import_queue = (config['asection']['sleeper.bulk.import.eks.job.queue.url']).rsplit('/', 1)[1]

    query_queue = None
    if 'sleeper.query.queue.url' in config['asection']:
        query_queue = (config['asection']['sleeper.query.queue.url']).rsplit('/', 1)[1]

    query_results_bucket = None
    if 'sleeper.query.results.bucket' in config['asection']:
        query_results_bucket = (config['asection']['sleeper.query.results.bucket'])

    dynamodb_query_tracker_table = None
    if 'sleeper.query.tracker.table.name' in config['asection']:
        dynamodb_query_tracker_table = (config['asection']['sleeper.query.tracker.table.name'])

    return (ingest_queue, emr_bulk_import_queue, persistent_emr_bulk_import_queue, eks_bulk_import_queue, query_queue,
            query_results_bucket, dynamodb_query_tracker_table)


def _write_and_upload_parquet(records_to_write: list, s3_file: str):
    """
    Creates a Parquet file from the user's records and uploads to their Sleeper databucket.

    :param records_to_write: list of the dictionaries containing the records to user wants to write to the Parquet file

    :param s3_file: name of the databucket concatenated with the name of the Parquet file
    """
    # Separates the argument into the databucket name and the filename they want to appear in the bucket
    databucket_name = s3_file.split('/', 1)[0]
    file_name = s3_file.split('/', 1)[1]

    with tempfile.NamedTemporaryFile() as fp:
        writer = ParquetSerialiser(fp)
        for record in records_to_write:
            writer.write_record(record)
        writer.write_tail()

        _s3.upload_file(fp.name, databucket_name, file_name)


def _ingest(table_name: str, files_to_ingest: list, ingest_queue: str, job_id: str = str(uuid.uuid4())):
    """
    Instructs Sleeper to ingest the given file from S3.

    :param table_name: table name to ingest to
    :param files_to_ingest: path to the file on the S3 databucket which is to be ingested
    :param ingest_queue: name of the Sleeper instance's ingest queue
    """
    if ingest_queue == None:
        raise Exception("Ingest queue is not defined - was the Ingest Stack deployed?")

    # Creates the ingest message and generates an ID
    ingest_message = {
        "id": job_id,
        "tableName": table_name,
        "files": files_to_ingest
    }

    # Converts ingest message to json and sends to the SQS queue
    ingest_message_json = json.dumps(ingest_message)
    logger.debug(f"Sending JSON message to queue {ingest_message_json}")
    ingest_queue_sqs = _sqs.get_queue_by_name(QueueName=ingest_queue)
    ingest_queue_sqs.send_message(MessageBody=ingest_message_json)


def _bulk_import(table_name: str, files_to_ingest: list,
                 emr_bulk_import_queue: str, persistent_emr_bulk_import_queue: str, eks_bulk_import_queue: str,
                 job_id: str, platform: str, platform_spec: dict, class_name: str):
    """
    Instructs Sleeper to bulk imoport the given files from S3.

    :param table_name: table name to bulk import to
    :param files_to_ingest: path to the file on the S3 databucket which is to be bulk imported
    :param emr_bulk_import_queue: name of the Sleeper instance's non-persistent EMR bulk import queue
    :param persistent_emr_bulk_import_queue: name of the Sleeper instance's persistent EMR bulk import queue
    :param eks_bulk_import_queue: name of the Sleeper instance's EKS bulk import queue
    :param job_id: the id of the bulk import job
    :param platform: the platform to use - either "EMR" or "PersistentEMR" or "EKS"
    :param platform_spec: a dict containing details of the platform to use - see docs/python-api.md
    """
    if platform != "EMR" and platform != "EKS" and platform != "PersistentEMR":
        raise Exception("Platform must be 'EMR' or 'PersistentEMR' or 'EKS'")

    if platform == "EMR":
        if emr_bulk_import_queue == None:
            raise Exception("EMR bulk import queue is not defined - was the EmrBulkImportStack deployed?")
        queue = emr_bulk_import_queue
    elif platform == "PersistentEMR":
        if persistent_emr_bulk_import_queue == None:
            raise Exception(
                "Persistent EMR bulk import queue is not defined - was the PersistentEmrBulkImportStack deployed?")
        queue = persistent_emr_bulk_import_queue
    else:
        if eks_bulk_import_queue == None:
            raise Exception("EKS bulk import queue is not defined - was the EksBulkImportStack deployed?")
        queue = eks_bulk_import_queue

    if job_id == None:
        job_id = str(uuid.uuid4())

    # Creates the ingest message and generates and ID
    bulk_import_message = {
        "id": job_id,
        "tableName": table_name,
        "files": files_to_ingest
    }
    if platform_spec != None and platform != "PersistentEMR":
        bulk_import_message["platformSpec"] = platform_spec
    if class_name != None:
        bulk_import_message["className"] = class_name

    # Converts bulk import message to json and sends to the SQS queue
    bulk_import_message_json = json.dumps(bulk_import_message)
    logger.debug(f"Sending JSON message to queue {bulk_import_message_json}")
    bulk_import_queue_sqs = _sqs.get_queue_by_name(QueueName=queue)
    bulk_import_queue_sqs.send_message(MessageBody=bulk_import_message_json)


def _receive_messages(self, query_id: str, timeout: int = DEFAULT_MAX_WAIT_TIME) -> List:
    """
    Polls the DynamoDB query tracker until the query is completed, then reads the results from S3.

    :param query_id: the id of the Sleeper query
    :param timeout: the maximum time in seconds to wait for a result

    :return: a list of the records the user queried for
    """
    # This while loop will poll the DynamoDB query tracker until the query is completed. Upon completion the
    # results will be read from S3 into a list and returned.
    results_table = _dynanmodb.Table(self._dynamodb_query_tracker_table)

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
        query_status_response = results_table.query(
            KeyConditionExpression=Key('queryId').eq(query_id) & Key('subQueryId').eq("-")
        )

        if len(query_status_response['Items']) == 0:
            continue

        lastKnownState = query_status_response['Items'][0]['lastKnownState']

        if lastKnownState == "FAILED":
            logger.debug("Query failed")
            raise RuntimeError("Query failed")
        elif lastKnownState == "COMPLETED":
            # Query results are put as Parquet files in the results bucket under "query-<query id>/"
            # (see Java class S3ResultsOutput for the precise formation of the location of the result files)
            results_files = []
            for object_summary in _s3_resource.Bucket(self._query_results_bucket).objects.filter(
                    Prefix=f"query-{query_id}"):
                logger.debug(f"Found {object_summary.key}")
                if object_summary.key.endswith(".parquet"):
                    results_files.append(object_summary.key)

            results = []
            for file in results_files:
                logger.debug(f"Opening file {self._query_results_bucket}/{file}")
                f = self._s3fs.open(f"{self._query_results_bucket}/{file}", 'rb')
                parq = ParquetDeserialiser()
                reader = parq.read(f)
                for r in reader:
                    results.append(r)

            logger.debug("Query has finished")
            return results
    raise RuntimeError("No results received from Sleeper within specified timeout.")


def _make_ingest_bucket_name(basename: str, table_name: str) -> str:
    """
    Returns the S3 bucket name that Sleeper will use for storing table data.

    :param basename: the Sleeper instance base name (sleeper.id)
    :param table_name: the table being worked with

    :return: S3 bucket name
    """
    return f"sleeper-{basename}-table-{table_name}"


def _make_ingest_s3_name(bucket: str) -> str:
    file_name: str = str(uuid.uuid4()) + ".parquet"
    databucket_file = f"{bucket}/for_ingest/{file_name}"
    return databucket_file
