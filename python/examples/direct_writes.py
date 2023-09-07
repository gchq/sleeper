#  Copyright 2022-2023 Crown Copyright
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
import random
import string

from sleeper.sleeper import SleeperClient

"""
This example shows how to write data to Sleeper via writing in batches. Data will not
be ingested to Sleeper until the batch writer is closed. If you structure your code as
shown, this will be automatic. Note that for large-scale imports, the bulk import method
is recommended.
"""


def random_string(size=6, chars=string.printable):
    """Create a random string of characters.

    :param size: length of string to generate
    :param chars: list of characters to choose from

    :return: random string
    """
    return ''.join(random.choice(chars) for _ in range(size))


# Create Sleeper instance with base name of install
my_sleeper = SleeperClient('my-sleeper-instance')

table_name = "my_table"

num_batches = 1

# Recommended method is to use the batch writer as follows:
with my_sleeper.create_batch_writer(table_name) as writer:
    # Create records in a loop
    for x in range(num_batches):

        records = []
        for y in range(1000):
            record = {"key": random_string(10, chars=string.digits),
                      "value": random_string(30)
                      }
            records.append(record)

        # Write this batch to Sleeper
        writer.write(records)

# Batch writer closed automatically
