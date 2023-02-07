#!/usr/bin/env bash
# Copyright 2022-2023 Crown Copyright
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

source "$(dirname "${BASH_SOURCE[0]}")/../propertyUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/../timeUtils.sh"
source "$(dirname "${BASH_SOURCE[0]}")/runTestUtils.sh"

SCRIPTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd ../.. && pwd)
GENERATED_DIR="$SCRIPTS_DIR/generated"
JARS_DIR="$SCRIPTS_DIR/jars"
TEMPLATES_DIR="$SCRIPTS_DIR/templates"
INSTANCE_PROPERTIES_TEMPLATE="$TEMPLATES_DIR/instanceproperties.template"
TABLE_PROPERTIES_TEMPLATE="$TEMPLATES_DIR/tableproperties.template"
SCHEMA_TEMPLATE="$TEMPLATES_DIR/schema.template"
INSTANCE_PROPERTIES="$GENERATED_DIR/instance.properties"

if compgen -G "$JARS_DIR/system-test-*-utility.jar" > /dev/null; then

  rm -r "${GENERATED_DIR:?}"/*

  cat "$INSTANCE_PROPERTIES_TEMPLATE" > "$INSTANCE_PROPERTIES"

  TABLE_DIR="$GENERATED_DIR/tables/table-name"
  mkdir -p "$TABLE_DIR"
  cat "$TABLE_PROPERTIES_TEMPLATE" > "$TABLE_DIR/table.properties"
  cat "$SCHEMA_TEMPLATE" > "$TABLE_DIR/schema.json"

  START=$(record_time)
  INSTANCE_ID=$(read_instance_property sleeper.id)
  END_INSTANCE_PROPERTY=$(record_time)
  TABLE_NAME=$(read_table_property table-name sleeper.table.name)
  END_TABLE_PROPERTY=$(record_time)
  echo "Read instance property took $(elapsed_time_str "$START" "$END_INSTANCE_PROPERTY")"
  echo "Read table property took $(elapsed_time_str "$END_INSTANCE_PROPERTY" "$END_TABLE_PROPERTY")"

  expect_string_for_actual "changeme" "$INSTANCE_ID"
  expect_string_for_actual "s3a://" "$(read_instance_property sleeper.filesystem)"

  expect_string_for_actual "changeme" "$TABLE_NAME"
  expect_string_for_actual "sleeper.statestore.dynamodb.DynamoDBStateStore" "$(read_table_property table-name sleeper.table.statestore.classname)"

else
  echo "Skipping property utils tests as utility jar is not present"
fi

end_tests
