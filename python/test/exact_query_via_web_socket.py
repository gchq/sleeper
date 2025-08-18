#!/usr/bin/env python3
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

import argparse
import asyncio
import json
import logging

from pq.parquet_serial import ParquetSerialiser
from sleeper.client import SleeperClient


async def run():
    logger.debug("Creating Sleeper Client")
    sleeper_client = SleeperClient(args.instance)

    logger.debug("Sending query via web socket")
    rows = await sleeper_client.web_socket_exact_key_query(table_name=args.table, keys=args.keys, query_id=args.queryid, strings_base64_encoded=args.base64_encoded)

    file_path = args.outdir + "/" + args.queryid + ".parquet"
    logger.debug(f"Saving resulst to disk at {file_path}")
    with open(file_path, "wb") as file:
        writer = ParquetSerialiser(file)
        for row in rows:
            writer.write_record(row)
        writer.write_tail()

    logger.debug("Query complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an exact query against Sleeper using Web Sockets")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--queryid", required=True)
    parser.add_argument("--base64_encoded", required=False, default=False)
    parser.add_argument("--keys", type=json.loads, required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
    logger = logging.getLogger(__name__)

    sleeper_client_logger = logging.getLogger("sleeper.client")
    if args.debug:
        sleeper_client_logger.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    else:
        sleeper_client_logger.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)

    asyncio.run(run())
