/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.clients.report;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Reports progress of a specified query.
 */
public class TrackedQueryProgressReport {

    private TrackedQueryProgressReport() {
    }

    public static void main(String[] args) throws QueryTrackerException {
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <query-id>");
        }
        String instanceId = args[0];
        String queryId = args[1];

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
            System.out.println(queryTracker.getStatus(queryId));
        }
    }
}
