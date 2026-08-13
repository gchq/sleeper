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

import logging

import requests

from sleeper.exceptions import SleeperApiError, SleeperConfigurationError
from sleeper.properties import CommonCdkProperty, CommonProperty, InstanceProperties, RestCdkProperty
from sleeper.rest.table import AddTableRequest, AddTableResponse, TableSchema
from sleeper.utils.signer import ApiGatewaySigner

logger = logging.getLogger(__name__)


class RestApiClient:
    """
    Client for interacting with the Sleeper REST API.

    Provides methods for performing operations against Sleeper resources,
    including table creation. Requests are signed using AWS Signature Version 4 (SigV4).
    """

    def __init__(self, instance_properties: InstanceProperties):
        """
        Create a REST API client.

        :param instance_properties: Properties for the deployed Sleeper instance.
        :raises SleeperConfigurationError: If the REST API stack has not been deployed.
        """

        self.instance_properties = instance_properties
        self.region = instance_properties.get(CommonCdkProperty.REGION)
        try:
            self.endpoint = instance_properties.get(RestCdkProperty.REST_BASE_URL)
        except KeyError as err:
            raise SleeperConfigurationError("The Sleeper REST API stack has not been deployed. REST API methods such as 'add_table' cannot be used until it is deployed.") from err

        self.signer = ApiGatewaySigner(region=self.region)

    def _add_table(self, request: AddTableRequest) -> AddTableResponse:
        """
        Create a new Sleeper table.

        :param request: The table creation request.
        :raises SleeperApiError: If the request returns a non-2xx status code.
        :return: Details of the created table.
        """

        url = self.endpoint + "/" + CommonProperty.ADD_TABLE_PATH
        body = request.to_json()
        logger.debug(f"Signing request {body} for url: {url}")
        signer = ApiGatewaySigner(region=self.region)

        headers = signer.sign(
            method="POST",
            url=url,
            headers={
                "Content-Type": "application/json",
            },
            body=body,
        )
        logger.debug(f"Headers: {headers}")

        logger.info(f"Creating table {request.properties.get('sleeper.table.name')}")
        response = requests.post(
            url,
            data=body,
            headers=headers,
            timeout=30,
        )

        self._raise_for_status(response=response)
        response = AddTableResponse.from_dict(response.json())
        logger.debug(f"Response: {response}")

        return response

    def add_table(self, table_name: str, schema: TableSchema, split_points: list | None = None) -> AddTableResponse:
        """
        Create a new Sleeper table.

        :param table_name: The name of the table to create.
        :param schema: The schema definition for the table.
        :param split_points: Optional split points used for initial partitioning.
        :raises SleeperApiError: If the request returns a non-2xx status code.
        :return: Details of the created table.
        """

        properties = {"sleeper.table.name": table_name}

        request = AddTableRequest(
            properties=properties,
            schema=schema,
            splitPoints=split_points,
        )
        logger.debug(f"AddTableRequest: {request}")
        return self._add_table(request=request)

    def _raise_for_status(self, response: requests.Response) -> None:
        """
        Raise a SleeperApiError for non-successful HTTP responses.

        :param response: The HTTP response to validate.
        :raises SleeperApiError: If the response returns a non-2xx status code.
        """
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            logger.debug(f"None 2xx status code. {response.json()}")
            raise SleeperApiError(
                status_code=response.status_code,
                message=response.reason,
                response_body=response.json(),
            ) from err
