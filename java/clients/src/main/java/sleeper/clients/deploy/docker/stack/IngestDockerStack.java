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

package sleeper.clients.deploy.docker.stack;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.tracker.job.DynamoDBIngestJobTrackerCreator;
import sleeper.ingest.tracker.task.DynamoDBIngestTaskTrackerCreator;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class IngestDockerStack implements DockerStack {
    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
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
            AmazonDynamoDB dynamoDB, SqsClient sqsClient) {
        return builder().instanceProperties(instanceProperties)
                .dynamoDB(dynamoDB).sqsClient(sqsClient)
                .build();
    }

    public void deploy() {
        DynamoDBIngestJobTrackerCreator.create(instanceProperties, dynamoDB);
        DynamoDBIngestTaskTrackerCreator.create(instanceProperties, dynamoDB);
        String queueName = "sleeper-" + instanceProperties.get(ID) + "-IngestJobQ";
        String queueUrl = sqsClient.createQueue(request -> request.queueName(queueName)).queueUrl();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, queueUrl);
    }

    public void tearDown() {
        DynamoDBIngestJobTrackerCreator.tearDown(instanceProperties, dynamoDB);
        DynamoDBIngestTaskTrackerCreator.tearDown(instanceProperties, dynamoDB);
        sqsClient.deleteQueue(request -> request.queueUrl(instanceProperties.get(INGEST_JOB_QUEUE_URL)));
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private SqsClient sqsClient;
        private AmazonDynamoDB dynamoDB;

        public Builder() {
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

        public IngestDockerStack build() {
            return new IngestDockerStack(this);
        }
    }
}
