#! /usr/bin/env python3

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

import asyncio
import json
import logging

from cmd_inputs import get_cmd_input
from process_query import process_query


def _save_results_to_file(results: str):
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
    cmd_input = get_cmd_input()

    use_envrion_auth = True
    results = asyncio.run(process_query(query=cmd_input.query, use_envrion_auth=use_envrion_auth))

    if cmd_input.save_results_to_file:
        _save_results_to_file(results)
