#! /usr/bin/env python3

import asyncio
import datetime
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from enum import Enum

import boto3
import websockets
from dotenv import load_dotenv


class MessageType(Enum):
    """
    Enum representing the different types of messages that can be received over the WebSocket connection.

    Attributes:
        COMPLETED (str): Indicates that the query has completed.
        ERROR (str): Indicates that an error has occurred.
        SUBQUERIES (str): Contains subquery identifiers related to the main query.
        RECORDS (str): Contains the records returned from the query.
    """

    COMPLETED = "completed"
    ERROR = "error"
    SUBQUERIES = "subqueries"
    RECORDS = "records"


class Query:
    """
    Represents a Sleeper query with specific parameters, capable of serializing itself into JSON for WebSocket communication.

    Attributes:
        query_id (str): Unique identifier for the query instance.
        table_name (str): Name of the database table to query.
        key (str): The key or column to filter on.
        min_value (str): The minimum value for the query region.
        max_value (str): The maximum value for the query region.
        min_inclusive (bool): Whether the minimum bound is inclusive.
        max_inclusive (bool): Whether the maximum bound is inclusive.
        strings_base64_encoded (bool): Whether string values are base64 encoded.
    """

    def __init__(
        self,
        table_name: str,
        key: str,
        min_value: str,
        max_value: str,
        min_inclusive: bool,
        max_inclusive: bool,
        strings_base64_encoded: bool,
    ):
        """
        Initializes a new Query object with specified parameters.

        Args:
            table_name (str): The name of the database table to query.
            key (str): The key or column to filter on.
            min_value (str): The minimum value for the query region.
            max_value (str): The maximum value for the query region.
            min_inclusive (bool): Whether the minimum bound is inclusive.
            max_inclusive (bool): Whether the maximum bound is inclusive.
            strings_base64_encoded (bool): Whether strings are encoded in base64.
        """
        self.query_id = str(uuid.uuid4())
        self.table_name = table_name
        self.key = key
        self.min_value = min_value
        self.max_value = max_value
        self.min_inclusive = min_inclusive
        self.max_inclusive = max_inclusive
        self.strings_base64_encoded = strings_base64_encoded

    def to_json(self):
        """
        Converts the Query object into a JSON-formatted string suitable for sending over WebSocket.

        Returns:
            str: JSON representation of the query object.
        """
        regions = [
            {
                "key": {
                    "min": self.min_value,
                    "minInclusive": self.min_inclusive,
                    "max": self.max_value,
                    "maxInclusive": self.max_inclusive,
                },
                "stringsBase64Encoded": self.strings_base64_encoded,
            }
        ]
        obj = {
            "queryId": self.query_id,
            "type": "Query",
            "tableName": self.table_name,
            "regions": regions,
            "resultsPublisherConfig": {},
        }
        return json.dumps(obj)


class CmdInputs:
    """
    A data class to hold command-line input parameters for the query execution.

    Attributes:
        table_name (str): Name of the database table to query.
        key (str): The key or column to filter on.
        min_value (str): Minimum value for the query range.
        max_value (str): Maximum value for the query range.
        min_inclusive (bool): Whether the minimum value is inclusive.
        max_inclusive (bool): Whether the maximum value is inclusive.
        strings_base64_encoded (bool): Whether string results are base64 encoded.
        save_results_to_file (bool): Whether to save query results to a file.
    """

    def __init__(
        self,
        table_name: str,
        key: str,
        min_value: str,
        max_value: str,
        min_inclusive: bool,
        max_inclusive: bool,
        strings_base64_encoded: bool,
        save_results_to_file: bool,
    ):
        """
        Initialize Command Inputs with provided parameters.

        Args:
            table_name (str): Name of the database table to query.
            key (str): The key or column to filter on.
            min_value (str): Minimum value for the query range.
            max_value (str): Maximum value for the query range.
            min_inclusive (bool): Whether the minimum value is inclusive.
            max_inclusive (bool): Whether the maximum value is inclusive.
            strings_base64_encoded (bool): Whether string results are base64 encoded.
            save_results_to_file (bool): Whether to save the query results to a file.
        """
        self.query = Query(
            table_name=table_name,
            key=key,
            min_value=min_value,
            max_value=max_value,
            min_inclusive=min_inclusive,
            max_inclusive=max_inclusive,
            strings_base64_encoded=strings_base64_encoded,
        )
        self.save_results_to_file = save_results_to_file


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


def _get_cmd_input() -> CmdInputs:
    """
    Prompt the user for query parameters and return a CmdInputs instance.

    Returns:
        CmdInputs: An instance containing the user-specified query configurations.
    """
    table_name = input("Table name? (Press Enter for default 'testing') ") or "testing"
    key = input("Key? (Press Enter for default 'key') ") or "key"
    exact = _get_boolean_input("Is the an exact query? (Please Enter for default True)", default=True)
    if exact:
        value = input("Value? ")
        min_value = value
        max_value = value
        min_inclusive = True
        max_inclusive = True
    else:
        min_value = input("Min value? ")
        max_value = input("Max value? ")
        min_inclusive = _get_boolean_input("Min inclusive? (Press Enter for default True) ", default=True)
        max_inclusive = _get_boolean_input("Max inclusive?  (Press Enter for default True) ", default=True)
    strings_base64_encoded = _get_boolean_input("Base64 encoded? (Press Enter for default False) ", default=False)
    save_results_to_file = _get_boolean_input("Save results to a file? (Press Enter for default True) ", default=True)

    return CmdInputs(
        table_name=table_name,
        key=key,
        min_value=min_value,
        max_value=max_value,
        min_inclusive=min_inclusive,
        max_inclusive=max_inclusive,
        strings_base64_encoded=strings_base64_encoded,
        save_results_to_file=save_results_to_file,
    )


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


def _get_boolean_input(prompt: str, default: bool = True) -> bool:
    """
    Prompt the user for a boolean input with a default value.

    Args:
        prompt (str): The message displayed to the user.
        default (bool, optional): The default value if user presses Enter. Defaults to True.

    Returns:
        bool: The user's input interpreted as a boolean.
    """
    user_input = input(prompt).strip().lower()

    if not user_input:
        return default

    if user_input in ("true", "yes", "1"):
        return True
    elif user_input in ("false", "no", "0"):
        return False
    else:
        logger.warning("Invalid input, defaulting to", default)
        return default


def save_results_to_file(results: str):
    """
    Save the query results to a JSON file.

    Args:
        results (dict): The dictionary containing the results to save.

    Raises:
        IOError: If there's an error writing to the file.
    """
    try:
        with open("results.json", "w") as file:
            logger.info("Saving results to results.json")
            json.dump(results, file)
    except IOError as e:
        logger.error(f"Failed to save results: {e}")


if __name__ == "__main__":
    """
    Main execution block for the script.
    Sets up logging, obtains user input, processes the query,
    and optionally saves the results to a file.
    """
    # Configure the logger
    logging.basicConfig(
        level=logging.INFO,  # Set the log level for the ROOT logger
        format="%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("sleeper.log"),
            logging.StreamHandler(),
        ],
    )

    logger = logging.getLogger("web_socket_query")
    logger.setLevel(logging.DEBUG)  # set the log level for the module
    cmd_input = _get_cmd_input()

    use_envrion_auth = True
    results = asyncio.run(process_query(query=cmd_input.query, use_envrion_auth=use_envrion_auth))

    if cmd_input.save_results_to_file:
        save_results_to_file(results)
