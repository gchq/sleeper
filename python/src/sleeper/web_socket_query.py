#  Copyright 2022-2026 Crown Copyright
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
import json
import logging
import time
from enum import Enum

import websockets

from sleeper.properties import CommonCdkProperty, InstanceProperties, QueryCdkProperty
from sleeper.query import Query
from sleeper.utils import ApiGatewaySigner

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
    """
    Executes Sleeper queries using the query WebSocket API.
    """

    def __init__(self, instance_properties: InstanceProperties | None = None, endpoint: str | None = None, region: str | None = None):
        """
        Create a query processor.

        Either instance_properties must be supplied, or both endpoint
        and region must be supplied.

        Args:
            instance_properties: Sleeper instance properties.
            endpoint: WebSocket endpoint URL.
            region: AWS region hosting the endpoint.

        Raises:
            ValueError: If invalid constructor arguments are supplied.
        """
        if (instance_properties is None and (endpoint is None or region is None)) or (instance_properties is not None and (endpoint is not None or region is not None)):
            raise ValueError("Either 'instance_properties' must be provided, or both 'endpoint' and 'region' must be provided.")
        if instance_properties:
            self.endpoint = instance_properties.get(QueryCdkProperty.QUERY_WEBSOCKET_URL)
            self.region = instance_properties.get(CommonCdkProperty.REGION)
        else:
            self.endpoint = endpoint
            self.region = region

        self.signer = ApiGatewaySigner(self.region)

    def _get_websocket_auth_headers(self) -> dict[str, str]:
        """
        Generate a signed WebSocket URL with SigV4 authentication and corresponding headers.

        Returns:
            Dictionary of signed request headers.
        """
        return self.signer.sign(method="GET", url=self.endpoint)

    async def process_query(self, query: Query) -> list:
        """
        Execute a query and return all result rows.

        Args:
            query: Query to execute.

        Returns:
            List of rows returned by the query.
        """
        start_time = time.perf_counter()

        headers = self._get_websocket_auth_headers()

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
                except websockets.exceptions.ConnectionClosedError:
                    logger.error("Connection closed")
                    break

                result_json = json.loads(response)

                try:
                    message = MessageType(result_json.get("message"))
                except Exception:
                    logger.exception("Error processing JSON message")
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
