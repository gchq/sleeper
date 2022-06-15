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

# TODO The conf below should be taken from the bulk import job platform spec
# or if that is not specified then it should be taken from the table properties
# and finally from the instance properties.

config_bucket=$1
jars_bucket=$2
version=$3
queue=$4

echo "Config bucket is ${config_bucket}"
echo "Jars bucket is ${jars_bucket}"
echo "Version is ${version}"
echo "Bulk import job queue url is ${queue}"

while true; do
	echo "Polling for messages with wait time of 20 seconds"
	message=$(aws sqs receive-message --queue-url "${queue}" --wait-time-seconds 20 --max-number-of-messages 1)
		
	if [[ ! -z "${message}" ]]; then
		echo "Received message ${message}"
		body=$(echo "${message}" | jq -r .Messages[0].Body | sed 's/ //g')
		spark-submit --deploy-mode cluster\
			--conf spark.shuffle.mapStatus.compression.codec=lz4\
			--conf spark.speculation=false\
			--conf spark.speculation.quantile=0.75\
			--conf spark.hadoop.fs.s3a.connection.maximum=25\
			--conf spark.yarn.submit.waitAppCompletion=false\
			--class sleeper.bulkimport.job.runner.dataframe.BulkImportJobDataframeRunner\
			/home/hadoop/bulk-import-runner-${version}.jar\
			${body}\
			${config_bucket}
		receipt_handle=$(echo "${message}" | jq -r .Messages[0].ReceiptHandle)
		aws sqs delete-message --queue-url "${queue}" --receipt-handle "${receipt_handle}"
                echo "Deleted message with receipt handle ${receipt_handle}"
	else
		echo "Empty"
	fi
done
