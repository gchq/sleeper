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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTrackerCreator;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class CompactionDockerStack implements DockerStack {
    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final AmazonDynamoDB dynamoDB;

    private CompactionDockerStack(Builder builder) {
        instanceProperties = builder.instanceProperties;
        sqsClient = builder.sqsClient;
        dynamoDB = builder.dynamoDB;
    }

    public static CompactionDockerStack from(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB, SqsClient sqsClient) {
        return builder().instanceProperties(instanceProperties).dynamoDB(dynamoDB).sqsClient(sqsClient)
                .build();
    }

    public void deploy() {
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoDB);
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoDB);
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqsClient.createQueue(request -> request
                .queueName("sleeper-" + instanceProperties.get(ID) + "-CompactionJobQ"))
                .queueUrl());
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL, sqsClient.createQueue(request -> request
                .queueName("sleeper-" + instanceProperties.get(ID) + "-PendingCompactionJobBatchQ"))
                .queueUrl());
    }

    @Override
    public void tearDown() {
        DynamoDBCompactionJobTrackerCreator.tearDown(instanceProperties, dynamoDB);
        DynamoDBCompactionTaskTrackerCreator.tearDown(instanceProperties, dynamoDB);
        sqsClient.deleteQueue(request -> request.queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL)));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private SqsClient sqsClient;
        private AmazonDynamoDB dynamoDB;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public CompactionDockerStack build() {
            return new CompactionDockerStack(this);
        }
    }
}
