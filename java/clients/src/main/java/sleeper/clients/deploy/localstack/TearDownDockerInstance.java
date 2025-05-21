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

package sleeper.clients.deploy.localstack;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.clients.deploy.localstack.stack.CompactionDockerStack;
import sleeper.clients.deploy.localstack.stack.ConfigurationDockerStack;
import sleeper.clients.deploy.localstack.stack.IngestDockerStack;
import sleeper.clients.deploy.localstack.stack.TableDockerStack;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;

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
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            tearDown(instanceId, s3Client, dynamoClient, sqsClient);
        }
    }

    public static void tearDown(String instanceId, S3Client s3Client, DynamoDbClient dynamoClient, SqsClient sqsClient) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

        ConfigurationDockerStack.from(instanceProperties, s3Client).tearDown();
        TableDockerStack.from(instanceProperties, s3Client, dynamoClient).tearDown();
        IngestDockerStack.from(instanceProperties, dynamoClient, sqsClient).tearDown();
        CompactionDockerStack.from(instanceProperties, dynamoClient, sqsClient).tearDown();
    }
}
