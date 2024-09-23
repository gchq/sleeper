/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.clients.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.QueryTrackerException;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);
            System.out.println(queryTracker.getStatus(queryId));
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
