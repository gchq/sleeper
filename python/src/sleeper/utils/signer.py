# Copyright 2022-2026 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.session import Session


class ApiGatewaySigner:
    """
    Signs API Gateway requests using AWS Signature Version 4 (SigV4).

    Supports signing requests for API Gateway HTTP, REST and WebSocket APIs.
    The returned headers can be supplied directly to an HTTP client or used
    as part of a WebSocket connection handshake.
    """

    def __init__(self, region: str):
        """
        Create a signer for API Gateway requests.

        :param region: The AWS region hosting the API Gateway endpoint.
        """

        self.region = region

    def sign(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        body: str | bytes | None = None,
    ) -> dict[str, str]:
        """
        Sign a request using AWS Signature Version 4.

        :param method: The HTTP method (GET, POST, PUT, DELETE, etc.).
        :param url: The fully qualified request URL.
        :param headers: Optional request headers.
        :param body: Optional request payload.
        :return: A dictionary containing the signed request headers.
        """

        request_headers = headers.copy() if headers else {}

        credentials = Session().get_credentials().get_frozen_credentials()

        request = AWSRequest(
            method=method,
            url=url,
            headers=request_headers,
            data=body,
        )

        SigV4Auth(
            credentials,
            "execute-api",
            self.region,
        ).add_auth(request)

        return dict(request.headers)
