#!/usr/bin/env bash
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

# This script builds and uploads docker images to ECR
set -e

if [ "$#" -ne 5 ]; then
  echo "usage $0 <instance id> <Docker registry> <version> <stacks being deployed> <docker base dir>"
  exit 1
fi

INSTANCE_ID=$1
DOCKER_REGISTRY=$2
VERSION=$3
STACKS=$4
BASE_DOCKERFILE_DIR=$5
REGION=$(echo ${DOCKER_REGISTRY} | sed -e "s/^.*\.dkr\.ecr\.\(.*\)\.amazonaws\.com/\1/")
STACKS=$(echo ${STACKS//,/ })
REPO_PREFIX=${DOCKER_REGISTRY}/${INSTANCE_ID}


echo "-------------------------------------------------------------------------------"
echo "Running Upload Docker Images"
echo "-------------------------------------------------------------------------------"

echo "INSTANCE_ID: ${INSTANCE_ID}"
echo "DOCKER_REGISTRY: ${DOCKER_REGISTRY}"
echo "STACKS: ${STACKS}"
echo "REGION: ${REGION}"
echo "BASE_DOCKERFILE_DIR: ${BASE_DOCKERFILE_DIR}"
echo "REPO_PREFIX: ${REPO_PREFIX}"
echo "VERSION: ${VERSION}"

Stacks_CompactionStack="compaction-job-execution"
Stacks_IngestStack="ingest"
Stacks_SystemTestStack="system-test"
Stacks_EksBulkImportStack="bulk-import-runner"

echo "Beginning docker build and push of images for the following stacks: ${STACKS}"

aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${DOCKER_REGISTRY}

for stack in ${STACKS}; do

    Key=Stacks_${stack}
    DIR=${!Key}
    
    if [[ ! -z "${DIR}" ]]; then
      echo "Building Stack: $stack"
      REPO=${DIR}

      # Check the return code
      # Do not fail the script, this creates the repository if needed
      set +e
      aws ecr describe-repositories --repository-names ${INSTANCE_ID}/${REPO} --no-cli-pager >/dev/null 2>&1
      STATUS=$?
      set -e

      # Create the docker repository if required
      if [ $STATUS -ne 0 ]; then
        echo "Creating repository ${INSTANCE_ID}/${REPO}"
        aws ecr create-repository --repository-name ${INSTANCE_ID}/${REPO} \
	      --image-scanning-configuration scanOnPush=true --no-cli-pager
      fi

      pushd ${BASE_DOCKERFILE_DIR}/${DIR}

      echo "Building and Pushing Docker image ${REPO} to repository ${INSTANCE_ID}/${REPO}"
      docker build -t ${REPO_PREFIX}/${REPO}:${VERSION} ./
      docker push ${REPO_PREFIX}/${REPO}:${VERSION}
	    popd
	fi
done
