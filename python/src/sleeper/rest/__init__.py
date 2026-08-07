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
"""
Public API for interacting with Sleeper REST services.

This module re-exports the primary client and data models used to create
and manage Sleeper tables via the REST API.

Exports:
    RestApiClient: Client for interacting with the Sleeper REST API.
    AddTableRequest: Request model used when creating a table.
    AddTableResponse: Response model returned after creating a table.
    TableSchema: Schema definition for a Sleeper table.
"""

from sleeper.rest.rest_client import RestApiClient
from sleeper.rest.table import AddTableRequest, AddTableResponse, TableSchema

__all__ = ("AddTableRequest", "AddTableResponse", "RestApiClient", "TableSchema")
