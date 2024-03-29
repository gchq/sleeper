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

package sleeper.clients.docker.stack;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;

public class CompactionDockerStack implements DockerStack {
    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonDynamoDB dynamoDB;

    private CompactionDockerStack(Builder builder) {
        instanceProperties = builder.instanceProperties;
        sqsClient = builder.sqsClient;
        dynamoDB = builder.dynamoDB;
    }

    public static CompactionDockerStack from(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB, AmazonSQS sqsClient) {
        return builder().instanceProperties(instanceProperties).dynamoDB(dynamoDB).sqsClient(sqsClient)
                .build();
    }

    public void deploy() {
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        sqsClient.createQueue(instanceProperties.get(COMPACTION_JOB_QUEUE_URL));
    }

    @Override
    public void tearDown() {
        DynamoDBCompactionJobStatusStoreCreator.tearDown(instanceProperties, dynamoDB);
        DynamoDBCompactionTaskStatusStoreCreator.tearDown(instanceProperties, dynamoDB);
        sqsClient.deleteQueue(instanceProperties.get(COMPACTION_JOB_QUEUE_URL));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private AmazonSQS sqsClient;
        private AmazonDynamoDB dynamoDB;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
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
