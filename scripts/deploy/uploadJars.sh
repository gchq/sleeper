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

# This script uploads the JARS to S3
set -e

if [ "$#" -ne 4 ]; then
  echo "usage $0 <jars bucket> <region> <version> <jar_dir>"
  exit 1
fi

JARS_BUCKET=$1
REGION=$2
VERSION=$3
JAR_DIR=$4

echo "-------------------------------------------------------------------------------"
echo "Running Upload Jars to S3"
echo "-------------------------------------------------------------------------------"

echo "JARS_BUCKET: ${JARS_BUCKET}"
echo "REGION: ${REGION}"
echo "VERSION: ${VERSION}"
echo "JAR_DIR: ${JAR_DIR}"

check_bucket_exists() {
  set +e
  aws s3api head-bucket --bucket "${JARS_BUCKET}" >/dev/null 2>&1
  STATUS=$?
  set -e
  echo "$STATUS"
}

if [ "$(check_bucket_exists)" -ne 0 ]; then
  echo "Creating JARs bucket"
  aws s3api create-bucket --acl private --bucket "${JARS_BUCKET}" --region "${REGION}" --create-bucket-configuration "LocationConstraint=${REGION}"
  aws s3api put-public-access-block --bucket "${JARS_BUCKET}" --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  while [ "$(check_bucket_exists)" -ne 0 ]; do
    sleep 5
  done
fi

echo "Uploading JARs"
aws s3 sync --size-only "${JAR_DIR}" "s3://${JARS_BUCKET}"
