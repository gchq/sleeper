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

package sleeper.clients.docker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.clients.docker.stack.CompactionDockerStack;
import sleeper.clients.docker.stack.ConfigurationDockerStack;
import sleeper.clients.docker.stack.IngestDockerStack;
import sleeper.clients.docker.stack.TableDockerStack;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class TearDownDockerInstance {
    private TearDownDockerInstance() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        if (System.getenv("AWS_ENDPOINT_URL") == null) {
            throw new IllegalArgumentException("Environment variable AWS_ENDPOINT_URL not set");
        }
        String instanceId = args[0];
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            tearDown(instanceId, s3Client, dynamoDBClient, sqsClient);
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }

    public static void tearDown(String instanceId, AmazonS3 s3Client, AmazonDynamoDB dynamoDB, SqsClient sqsClient) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

        ConfigurationDockerStack.from(instanceProperties, s3Client).tearDown();
        TableDockerStack.from(instanceProperties, s3Client, dynamoDB).tearDown();
        IngestDockerStack.from(instanceProperties, dynamoDB, sqsClient).tearDown();
        CompactionDockerStack.from(instanceProperties, dynamoDB, sqsClient).tearDown();
    }
}
