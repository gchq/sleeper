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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStoreCreator;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestDockerStack implements DockerStack {
    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final AmazonDynamoDB dynamoDB;

    private IngestDockerStack(Builder builder) {
        instanceProperties = builder.instanceProperties;
        sqsClient = builder.sqsClient;
        dynamoDB = builder.dynamoDB;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static IngestDockerStack from(
            InstanceProperties instanceProperties,
            AmazonDynamoDB dynamoDB, AmazonSQS sqsClient) {
        return builder().instanceProperties(instanceProperties)
                .dynamoDB(dynamoDB).sqsClient(sqsClient)
                .build();
    }

    public void deploy() {
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        sqsClient.createQueue(instanceProperties.get(INGEST_JOB_QUEUE_URL));
    }

    public void tearDown() {
        DynamoDBIngestJobStatusStoreCreator.tearDown(instanceProperties, dynamoDB);
        DynamoDBIngestTaskStatusStoreCreator.tearDown(instanceProperties, dynamoDB);
        sqsClient.deleteQueue(instanceProperties.get(INGEST_JOB_QUEUE_URL));
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private AmazonSQS sqsClient;
        private AmazonDynamoDB dynamoDB;

        public Builder() {
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

        public IngestDockerStack build() {
            return new IngestDockerStack(this);
        }
    }
}
