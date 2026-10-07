#!/usr/bin/env bash
# Copyright 2022-2026 Crown Copyright
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

set -euo pipefail
unset CDPATH

THIS_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$(dirname "${THIS_DIR}")")

# The images are several GB, so in CI we remove each one after reading it to save disk space.
# Locally we keep them, so they don't need pulling again on the next run.
REMOVE_IMAGES=false
if [ "${1:-}" == "--remove-images" ]; then
    REMOVE_IMAGES=true
elif [ $# -gt 0 ]; then
    echo "Usage: $0 [--remove-images]"
    exit 1
fi

# Which EMR image the bulk import AWS SDK version must match: serverless or eks.
# The other image is also checked, but a mismatch there only produces a warning.
REFERENCE_PLATFORM=serverless

# The instance property sleeper.bulk.import.emr.serverless.architecture is currently constrained to X86_64,
# so we only read the amd64 images.
DOCKER_PLATFORM=linux/amd64

# Read the EMR release label from the generated instance properties template, which is kept in sync with the
# default of sleeper.default.table.bulk.import.emr.release.label. EMR Serverless inherits this as its default.
EMR_RELEASE=$(grep -oP '(?<=^# sleeper\.default\.table\.bulk\.import\.emr\.release\.label=).*' \
    "${PROJECT_ROOT}/example/full/instance.properties")
echo "EMR release label: $EMR_RELEASE"

# The EKS image is set separately in the Dockerfile we build from, so check it uses the same release
EKS_IMAGE=$(grep -oP '(?<=^ARG BASE_IMAGE=).*' "${PROJECT_ROOT}/java/bulk-import/bulk-import-eks/docker/eks/Dockerfile")
EKS_RELEASE=$(echo "$EKS_IMAGE" | grep -oP 'emr-[0-9.]+(?=:)')
if [ "$EKS_RELEASE" != "$EMR_RELEASE" ]; then
    echo "EKS Dockerfile uses EMR release $EKS_RELEASE but the default EMR release label is $EMR_RELEASE"
    exit 1
fi

# We don't build from the EMR Serverless image, as EMR Serverless runs its own runtime for the release label.
# The latest image is the closest match to that.
SERVERLESS_IMAGE="public.ecr.aws/emr-serverless/spark/${EMR_RELEASE}:latest"
SERVERLESS_JAR_GLOB="/usr/share/aws/emr/serverless-goodies/lib/emr-serverless-spark-goodies-*.jar"
EKS_JAR_GLOB="/usr/share/aws/aws-java-sdk-v2/aws-sdk-java-bundle-*.jar"

TMP_DIR=$(mktemp -d)
CONTAINER=""
cleanup() {
    rm -rf "$TMP_DIR"
    if [ -n "$CONTAINER" ]; then
        docker rm "$CONTAINER"
    fi
}
trap cleanup EXIT

# Copies the jar holding the AWS SDK out of the image and reads the SDK version from it into SDK_VERSION.
# This only needs bash and ls in the image, as the EMR on EKS image has no unzip.
read_sdk_version() {
    local image=$1
    local jar_glob=$2
    echo "Pulling $image"
    docker pull --platform "$DOCKER_PLATFORM" "$image"
    echo "Finding jar matching $jar_glob in $image"
    local jars
    jars=$(docker run --rm --init --platform "$DOCKER_PLATFORM" --entrypoint /bin/bash "$image" -c "ls $jar_glob")
    echo "$jars"
    if [ "$(echo "$jars" | wc -l)" != "1" ]; then
        echo "Expected exactly one jar matching $jar_glob in $image"
        exit 1
    fi
    echo "Copying $jars out of $image"
    CONTAINER=$(docker create --platform "$DOCKER_PLATFORM" "$image")
    docker cp "$CONTAINER:$jars" "$TMP_DIR/sdk.jar"
    docker rm "$CONTAINER"
    CONTAINER=""
    if [ "$REMOVE_IMAGES" == "true" ]; then
        echo "Removing $image"
        docker rmi "$image"
    fi
    echo "Reading sdk-core pom.properties from copied jar"
    unzip -p "$TMP_DIR/sdk.jar" META-INF/maven/software.amazon.awssdk/sdk-core/pom.properties \
        > "$TMP_DIR/pom.properties"
    cat "$TMP_DIR/pom.properties"
    SDK_VERSION=$(grep -oP '(?<=^version=).*' "$TMP_DIR/pom.properties")
}

read_sdk_version "$SERVERLESS_IMAGE" "$SERVERLESS_JAR_GLOB"
SERVERLESS_AWS_VERSION=$SDK_VERSION
read_sdk_version "$EKS_IMAGE" "$EKS_JAR_GLOB"
EKS_AWS_VERSION=$SDK_VERSION

echo "Reading version from Maven pom.xml"
pushd "${PROJECT_ROOT}/java" > /dev/null
PINNED_AWS_VERSION=$(mvn help:evaluate -Dexpression=aws-java-sdk-v2.bulk-import.version -q -DforceStdout)
popd > /dev/null

echo "AWS SDK version in $SERVERLESS_IMAGE: $SERVERLESS_AWS_VERSION"
echo "AWS SDK version in $EKS_IMAGE: $EKS_AWS_VERSION"
echo "Bulk import version of AWS SDK: $PINNED_AWS_VERSION"

if [ "$REFERENCE_PLATFORM" == "serverless" ]; then
    REFERENCE_IMAGE=$SERVERLESS_IMAGE
    REFERENCE_AWS_VERSION=$SERVERLESS_AWS_VERSION
    OTHER_IMAGE=$EKS_IMAGE
    OTHER_AWS_VERSION=$EKS_AWS_VERSION
else
    REFERENCE_IMAGE=$EKS_IMAGE
    REFERENCE_AWS_VERSION=$EKS_AWS_VERSION
    OTHER_IMAGE=$SERVERLESS_IMAGE
    OTHER_AWS_VERSION=$SERVERLESS_AWS_VERSION
fi

if [ "$OTHER_AWS_VERSION" != "$PINNED_AWS_VERSION" ]; then
    echo "::warning::Bulk import AWS SDK version is $PINNED_AWS_VERSION but $OTHER_IMAGE provides $OTHER_AWS_VERSION"
fi

if [ "$REFERENCE_AWS_VERSION" != "$PINNED_AWS_VERSION" ]; then
    echo "Bulk import AWS SDK version is $PINNED_AWS_VERSION but $REFERENCE_IMAGE provides $REFERENCE_AWS_VERSION."
    echo "This change can be verified with a successful run of EmrServerlessBulkImportST."
    exit 1
else
    echo "Versions match"
fi
