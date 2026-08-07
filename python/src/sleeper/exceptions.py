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


class SleeperError(Exception):
    """Base exception for the Sleeper SDK."""


class SleeperApiError(SleeperError):
    """Returned when a Sleeper API request fails."""

    def __init__(
        self,
        status_code: int,
        message: str,
        response_body: str | None = None,
    ):
        self.status_code = status_code
        self.response_body = response_body
        self.message = response_body.get("message")

        super().__init__(f"HTTP {status_code}: {message}")


class SleeperConfigurationError(SleeperError):
    def __init__(
        self,
        message: str,
    ):
        super().__init__(f"Configuration Error: {message}")
