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
package sleeper.clients.deploy.localstack.stack;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.trackerv2.job.DynamoDBIngestJobTrackerCreator;
import sleeper.ingest.trackerv2.task.DynamoDBIngestTaskTrackerCreator;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class IngestDockerStack {
    private final InstanceProperties instanceProperties;
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;

    private IngestDockerStack(InstanceProperties instanceProperties, DynamoDbClient dynamoClient, SqsClient sqsClient) {
        this.instanceProperties = instanceProperties;
        this.dynamoClient = dynamoClient;
        this.sqsClient = sqsClient;
    }

    public static IngestDockerStack from(
            InstanceProperties instanceProperties,
            DynamoDbClient dynamoClient, SqsClient sqsClient) {
        return new IngestDockerStack(instanceProperties, dynamoClient, sqsClient);
    }

    public void deploy() {
        DynamoDBIngestJobTrackerCreator.create(instanceProperties, dynamoClient);
        DynamoDBIngestTaskTrackerCreator.create(instanceProperties, dynamoClient);
        String queueName = "sleeper-" + instanceProperties.get(ID) + "-IngestJobQ";
        String queueUrl = sqsClient.createQueue(request -> request.queueName(queueName)).queueUrl();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, queueUrl);
    }

    public void tearDown() {
        DynamoDBIngestJobTrackerCreator.tearDown(instanceProperties, dynamoClient);
        DynamoDBIngestTaskTrackerCreator.tearDown(instanceProperties, dynamoClient);
        sqsClient.deleteQueue(request -> request.queueUrl(instanceProperties.get(INGEST_JOB_QUEUE_URL)));
    }

}
