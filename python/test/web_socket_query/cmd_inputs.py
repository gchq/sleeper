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

import logging

from query import Query

logger = logging.getLogger("cmd_inputs")


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


def get_boolean_input(prompt: str, default: bool = True) -> bool:
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


def get_cmd_input() -> CmdInputs:
    """
    Prompt the user for query parameters and return a CmdInputs instance.

    Returns:
        CmdInputs: An instance containing the user-specified query configurations.
    """
    table_name = input("Table name? (Press Enter for default 'testing') ") or "testing"
    key = input("Key? (Press Enter for default 'key') ") or "key"
    exact = get_boolean_input("Is the an exact query? (Please Enter for default True) ", default=True)
    if exact:
        value = input("Value? ")
        min_value = value
        max_value = value
        min_inclusive = True
        max_inclusive = True
    else:
        min_value = input("Min value? ")
        max_value = input("Max value? ")
        min_inclusive = get_boolean_input("Min inclusive? (Press Enter for default True) ", default=True)
        max_inclusive = get_boolean_input("Max inclusive?  (Press Enter for default True) ", default=True)
    strings_base64_encoded = get_boolean_input("Base64 encoded? (Press Enter for default False) ", default=False)
    save_results_to_file = get_boolean_input("Save results to a file? (Press Enter for default True) ", default=True)

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
