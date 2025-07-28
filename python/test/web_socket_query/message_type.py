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

from enum import Enum


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
