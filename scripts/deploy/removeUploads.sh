#!/bin/bash
# Copyright 2022 Crown Copyright
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

# This script removes the Jars and ECR repositories which were uploaded as part of the setup.
set -e

if [ "$#" -ne 1 ]; then
  echo "usage $0 <system-test-properties>"
  exit 1
fi

PROPERTIES=$1

echo "Tearing down S3 Jars Bucket"
BUCKET=$(grep "sleeper.jars.bucket=" ${PROPERTIES} | sed -e "s/\(.*=\)\(.*\)/\2/")
echo "BUCKET:${BUCKET}"
aws s3 rb s3://${BUCKET} --force

STACKS=`grep sleeper.optional.stacks ${PROPERTIES} | cut -d'=' -f2`
STACKS=$(echo ${STACKS//,/ })
echo "Stacks:${STACKS}"

declare -A LOOKUP
LOOKUP=( \
["CompactionStack"]="sleeper.compaction.repo=" \
["IngestStack"]="sleeper.ingest.repo=" \
["SystemTestStack"]="sleeper.systemtest.repo=" \
["EksBulkImportStack"]="sleeper.bulk.import.eks.repo=")

echo "Removing ECR images"
for stack in ${STACKS}; do
    PROPERTY=${LOOKUP[${stack}]}
    if [[ ! -z "${PROPERTY}" ]]; then
      REPO="$(grep "${PROPERTY}" ${PROPERTIES} | sed -e "s/\(.*=\)\(.*\)/\2/")"
      echo ${REPO}
      aws ecr delete-repository --repository-name=${REPO} --force --no-cli-pager
    fi
done
