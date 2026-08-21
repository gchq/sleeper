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

"""Exceptions raised by the Sleeper SDK."""

from typing import Any


class SleeperError(Exception):
    """Base exception for all Sleeper SDK errors."""


class SleeperApiError(SleeperError):
    """Raised when a Sleeper API request returns an error response."""

    def __init__(
        self,
        status_code: int,
        message: str,
        response_body: dict[str, Any] | None = None,
    ):
        """
        Create an API error.

        :param status_code: HTTP status code returned by the API.
        :param message: HTTP reason phrase or fallback error message.
        :param response_body: Optional response body returned by the API.
        """
        self.status_code = status_code
        self.response_body = response_body

        api_message = response_body.get("message") if response_body and "message" in response_body else message

        self.message = api_message

        super().__init__(f"HTTP {status_code}: {api_message}")


class SleeperConfigurationError(SleeperError):
    """Raised when the Sleeper client is incorrectly configured."""
