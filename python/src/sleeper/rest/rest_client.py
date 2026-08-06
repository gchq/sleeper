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

from sleeper.exceptions import SleeperApiError
from sleeper.properties import CommonCdkProperty, CommonProperty, InstanceProperties, RestCdkProperty
from sleeper.rest.table import AddTableRequest, AddTableResponse, TableSchema
from sleeper.utils.signer import ApiGatewaySigner

logger = logging.getLogger(__name__)


class RestApiClient:
    def __init__(self, instance_properties: InstanceProperties):

        self.instance_properties = instance_properties
        self.region = instance_properties.get(CommonCdkProperty.REGION)
        self.endpoint = instance_properties.get(RestCdkProperty.REST_BASE_URL)

        self.signer = ApiGatewaySigner(region=self.region)

    def _add_table(self, request: AddTableRequest) -> AddTableResponse:
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

    def add_table(self, table_name: str, schema: TableSchema, split_points: list) -> AddTableResponse:

        properties = {"sleeper.table.name": table_name}

        request = AddTableRequest(
            properties=properties,
            schema=schema,
            splitPoints=split_points,
        )
        logger.debug(f"AddTableRequest: {request}")
        return self._add_table(request=request)

    def _raise_for_status(self, response: requests.Response) -> None:
        try:
            response.raise_for_status()

        except requests.HTTPError as err:
            raise SleeperApiError(
                status_code=response.status_code,
                message=response.reason,
                response_body=response.text,
            ) from err
