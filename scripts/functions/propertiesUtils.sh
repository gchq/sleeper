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

copy_all_properties() {
  THIS_DIR=$1;
  GENERATED_DIR=$2;
  TEMPLATES_DIR=$3

  copy_properties "$@" "instance.properties" "instanceproperties.template"
  copy_properties "$@" "tags.properties" "tags.template"
  copy_properties "$@" "table.properties" "tableproperties.template"
  copy_properties "$@" "schema.json" "schema.template"
}
copy_properties() {
  THIS_DIR=$1;
  GENERATED_DIR=$2;
  TEMPLATES_DIR=$3;
  PROPERTIES_FILE=$4;
  TEMPLATE_FILE=$5;
  if [ ! -f "${THIS_DIR}/${PROPERTIES_FILE}" ]; then
    cp "${TEMPLATES_DIR}/${TEMPLATE_FILE}" "${GENERATED_DIR}/${PROPERTIES_FILE}"
  else
    cp "${THIS_DIR}/${PROPERTIES_FILE}" "${GENERATED_DIR}/${PROPERTIES_FILE}"
  fi
}