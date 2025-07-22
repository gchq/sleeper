#! /usr/bin/env python3

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
import os
import time

import boto3
import websockets
from dotenv import load_dotenv
from message_type import MessageType
from query import Query

logger = logging.getLogger("process_query")
logger.setLevel(logging.DEBUG)


def _construct_websocket_endpoint(host: str, stage: str) -> str:
    """
    Constructs a WebSocket endpoint URL given the host and deployment stage.

    Args:
        host (str): The host domain, e.g., 'api-id.execute-api.region.amazonaws.com'.
        stage (str): The deployment stage, e.g., 'prod', 'dev', or 'live'.

    Returns:
        str: The complete WebSocket URL in the format 'wss://{host}/{stage}'.
    """
    endpoint = f"wss://{host}/{stage}"
    return endpoint


def _get_signature_key(key: str, date_stamp: str, region_name: str, service_name: str) -> bytes:
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


def _get_websocket_auth_header_from_envrion() -> tuple[str, str]:
    """
    Generate a signed WebSocket URL with SigV4 authentication and corresponding headers.
    Returns:
        Tuple containing:
            - The signed WebSocket URL (str)
            - Headers (dict) including 'X-Amz-Date', 'Authorization', and possibly 'X-Amz-Security-Token'
    """
    load_dotenv()
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
    aws_region = os.environ.get("AWS_REGION") or "eu-west-2"
    if "AWS_REGION" not in os.environ:
        logger.info("AWS_REGION not set in environment, defaulting to 'eu-west-2'")
    api_id = os.environ.get("AWS_API_GATEWAY_ID")
    stage = os.environ.get("AWS_API_GATEWAY_STAGE") or "live"
    if "AWS_API_GATEWAY_STAGE" not in os.environ:
        logger.info("AWS_API_GATEWAY_STAGE not set in environment, defaulting to 'live'")
    host = f"{api_id}.execute-api.{aws_region}.amazonaws.com"
    endpoint = _construct_websocket_endpoint(host=host, stage=stage)

    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        aws_session_token=aws_session_token,
        region_name=aws_region,
    )

    session.get_credentials()

    # Create a low-level client with sigv4 signing
    boto3.client(
        "apigatewaymanagementapi",
        region_name=aws_region,
        endpoint_url=endpoint,
    )

    # Get current timestamp
    t = datetime.datetime.now(datetime.timezone.utc)
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")

    # Create canonical request
    method = "GET"
    canonical_uri = f"/{stage}"
    canonical_querystring = ""
    canonical_headers = f"host:{host}\n"
    signed_headers = "host"
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_request = f"{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

    # Create string to sign
    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{aws_region}/execute-api/aws4_request"
    string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"

    # Calculate signature
    signing_key = _get_signature_key(aws_secret_key, date_stamp, aws_region, "execute-api")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    # Create authorization header
    authorization_header = f"{algorithm} Credential={aws_access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"

    # Create headers for WebSocket connection
    headers = {"X-Amz-Date": amz_date, "Authorization": authorization_header}

    if aws_session_token:
        headers["X-Amz-Security-Token"] = aws_session_token

    # Return the endpoint and authenticated header
    return endpoint, headers


async def process_query(query: Query, use_envrion_auth: bool) -> dict:
    """
    Asynchronously processes a query over a WebSocket connection.

    Args:
        query (Query): The query object containing query.
        use_envrion_auth (bool): Whether to use environment-based WebSocket authentication.

    Returns:
        dict: A dictionary containing accumulated results under the key 'results'.
    """
    if use_envrion_auth:
        # Get signed URL and headers
        endpoint, headers = _get_websocket_auth_header_from_envrion()

    start_time = time.perf_counter()

    async with websockets.connect(endpoint, additional_headers=headers) as websocket:
        """signed_url = other_auth()
        logger.warning(signed_url)
        async with websockets.connect(signed_url) as websocket:"""
        await websocket.send(query.to_json())
        logger.debug(f"Sending message: {query.to_json()}")

        total_results = 0
        results_accumulator = {"results": []}

        while True:
            logger.debug("Waiting for results")
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
            elif message == MessageType.RECORDS:
                try:
                    records = result_json["records"]
                except KeyError as err:
                    logger.error("Error processing records")
                    logger.error(err.with_traceback())
                    continue
                logger.debug(records)
                results_accumulator["results"].extend(records)
                total_results += len(records)

            logger.info(f"Found a total of {total_results} results")

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.info(f"Query took: {elapsed_time:.4f} seconds")
        return results_accumulator
