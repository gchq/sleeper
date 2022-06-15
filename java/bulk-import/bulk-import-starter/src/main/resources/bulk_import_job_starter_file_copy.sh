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

jars_bucket=$1
version=$2
echo "Jars bucket is ${jars_bucket}"
echo "Version is ${version}"

aws s3 cp s3://${jars_bucket}/bulk_import_job_starter_${version}.sh /home/hadoop/bulk_import_job_starter.sh
chmod 777 /home/hadoop/bulk_import_job_starter.sh
echo "Copied s3://${jars_bucket}/bulk_import_job_starter_${version}.sh to /home/hadoop/bulk_import_job_starter.sh"

aws s3 cp s3://${jars_bucket}/bulk-import-runner-${version}.jar /home/hadoop/bulk-import-runner-${version}.jar
chmod 777 /home/hadoop/bulk-import-runner-${version}.jar
echo "Copied s3://${jars_bucket}/bulk-import-runner-${version}.jar to /home/hadoop/bulk-import-runner-${version}.jar"
