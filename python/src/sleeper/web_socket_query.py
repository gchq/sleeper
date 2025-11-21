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

import asyncio
import datetime
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from enum import Enum

import boto3
import websockets

from sleeper.properties.cdk_defined_properties import CommonCdkProperty, QueryCdkProperty
from sleeper.properties.instance_properties import InstanceProperties
from sleeper.query import Query

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """
    Enum representing the different types of messages that can be received over the WebSocket connection.

    Attributes:
        COMPLETED (str): Indicates that the query has completed.
        ERROR (str): Indicates that an error has occurred.
        SUBQUERIES (str): Contains subquery identifiers related to the main query.
        ROWS (str): Contains the rows returned from the query.
    """

    COMPLETED = "completed"
    ERROR = "error"
    SUBQUERIES = "subqueries"
    ROWS = "rows"


class WebSocketQueryProcessor:
    def __init__(self, instance_properties: InstanceProperties = None, endpoint: str = None, region: str = None):
        # Validate input
        if (instance_properties is None and (endpoint is None or region is None)) or (instance_properties is not None and (endpoint is not None or region is not None)):
            raise ValueError("Either 'instance_properties' must be provided, or both 'endpoint' and 'region' must be provided.")
        if instance_properties:
            self.endpoint = instance_properties.get(QueryCdkProperty.QUERY_WEBSOCKET_URL)
            self.region = instance_properties.get(CommonCdkProperty.REGION)
        else:
            self.endpoint = endpoint
            self.region = region

    def _get_signature_key(self, key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
        """
        Generate a signing key for AWS SigV4.

        Args:
            key (str): The AWS secret access key.
            date_stamp (str): The date in YYYYMMDD format.
            region_name (str): The AWS region.
            service_name (str): The AWS service.

        Returns:
            bytes: The derived signing key.
        """
        k_date = hmac.new(f"AWS4{key}".encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
        k_region = hmac.new(k_date, region_name.encode("utf-8"), hashlib.sha256).digest()
        k_service = hmac.new(k_region, service_name.encode("utf-8"), hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        return k_signing

    def _get_websocket_auth_header(self) -> str:
        """
        Generate a signed WebSocket URL with SigV4 authentication and corresponding headers.

        Returns:
            Tuple containing:
                - The signed WebSocket URL (str)
                - Headers (dict) including 'X-Amz-Date', 'Authorization', and possibly 'X-Amz-Security-Token'
        """

        boto_session = boto3.Session()
        credentials = boto_session.get_credentials()

        # Create a low-level client with sigv4 signing
        boto3.client(
            "apigatewaymanagementapi",
            region_name=self.region,
            endpoint_url=self.endpoint,
        )

        # Get current timestamp
        t = datetime.datetime.now(datetime.timezone.utc)
        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = t.strftime("%Y%m%d")

        # Parse the URL
        parsed_url = urllib.parse.urlparse(self.endpoint)

        # Host
        host = parsed_url.netloc

        # Create canonical request
        method = "GET"
        canonical_uri = parsed_url.path
        canonical_querystring = ""
        canonical_headers = f"host:{host}\n"
        signed_headers = "host"
        payload_hash = hashlib.sha256(b"").hexdigest()
        canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

        # Create string to sign
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/execute-api/aws4_request"
        string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"

        # Calculate signature
        signing_key = self._get_signature_key(credentials.secret_key, date_stamp, self.region, "execute-api")
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        # Create authorization header
        authorization_header = f"{algorithm} Credential={credentials.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

        # Create headers for WebSocket connection
        headers = {"X-Amz-Date": amz_date, "Authorization": authorization_header}

        if credentials.token:
            headers["X-Amz-Security-Token"] = credentials.token

        return headers

    async def process_query(self, query: Query) -> dict:
        """
        Asynchronously processes a query over a WebSocket connection.

        Args:
            query (QueWebSocketQuery): The query object containing query.

        Returns:
            dict: A dictionary containing accumulated results under the key 'results'.
        """

        start_time = time.perf_counter()
        headers = self._get_websocket_auth_header()
        logger.debug(f"Websocket URL: {self.endpoint}")
        async with websockets.connect(self.endpoint, additional_headers=headers) as websocket:
            query_json = query.to_json()
            logger.debug(f"Sending message: {query_json}")
            await websocket.send(query_json)
            total_results = 0
            results = []

            while True:
                logger.info("Waiting for results")
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=30)
                except asyncio.TimeoutError:
                    logger.error("Timeout occurred while waiting for response.")
                    break
                except websocket.exception.ConnectClosedError:
                    logger.error("Connection closed")
                    continue

                result_json = json.loads(response)
                try:
                    message = MessageType(result_json.get("message"))
                except Exception as e:
                    logger.error("Error processing Json message")
                    logger.error(e.with_traceback())
                    continue

                if message == MessageType.ERROR:
                    logger.error("Received message of type 'error'")
                    logger.error(result_json.get("error"))
                    break
                elif message == MessageType.COMPLETED:
                    logger.info("Query Completed")
                    break
                elif message == MessageType.SUBQUERIES:
                    query_ids = result_json.get("queryIds")
                    for subquery in query_ids:
                        logger.info(f"Subquery ID: {subquery}")
                elif message == MessageType.ROWS:
                    try:
                        rows = result_json["rows"]
                    except KeyError as err:
                        logger.error("Error processing rows")
                        logger.error(err.with_traceback())
                        continue
                    for row in rows:
                        results.append(row)
                    total_results += len(rows)

                logger.info(f"Found a total of {total_results} results")

            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            logger.info(f"Query took: {elapsed_time:.4f} seconds")
            return results
