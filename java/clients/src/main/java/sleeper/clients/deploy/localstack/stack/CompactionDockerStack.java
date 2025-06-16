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

import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTrackerCreator;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class CompactionDockerStack {
    private final InstanceProperties instanceProperties;
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;

    private CompactionDockerStack(InstanceProperties instanceProperties, DynamoDbClient dynamoClient, SqsClient sqsClient) {
        this.instanceProperties = instanceProperties;
        this.dynamoClient = dynamoClient;
        this.sqsClient = sqsClient;
    }

    public static CompactionDockerStack from(
            InstanceProperties instanceProperties,
            DynamoDbClient dynamoClient, SqsClient sqsClient) {
        return new CompactionDockerStack(instanceProperties, dynamoClient, sqsClient);
    }

    public void deploy() {
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClient);
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoClient);
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqsClient.createQueue(request -> request
                .queueName("sleeper-" + instanceProperties.get(ID) + "-CompactionJobQ"))
                .queueUrl());
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL, sqsClient.createQueue(request -> request
                .queueName("sleeper-" + instanceProperties.get(ID) + "-PendingCompactionJobBatchQ"))
                .queueUrl());
    }

    public void tearDown() {
        DynamoDBCompactionJobTrackerCreator.tearDown(instanceProperties, dynamoClient);
        DynamoDBCompactionTaskTrackerCreator.tearDown(instanceProperties, dynamoClient);
        sqsClient.deleteQueue(request -> request.queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL)));
    }

}
