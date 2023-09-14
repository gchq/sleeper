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
from sleeper.sleeper import SleeperClient

first_record = {
    'key': 'example-key',
    'value': 'example-value'
}

second_record = {
    'key': 'example-key2',
    'value': 'example-value2'
}

table_name = "my_table"

records_list = [first_record, second_record]

# Create Sleeper instance with base name of install
my_sleeper = SleeperClient('my-sleeper-instance')
my_sleeper.write_single_batch(table_name, records_list)

# These queries will not return results straight away, it can take a few minutes before
# Sleeper ingests data!

# Look up a result using a single key
print("Performing exact key query...")
print(my_sleeper.exact_key_query(table_name, {"key": ["akey", "anotherkey"]}))

# And retrieve a key range
print("Performing range key query...")
print(my_sleeper.range_key_query(table_name, [{"key": ["a", True, "c", False]}]))
