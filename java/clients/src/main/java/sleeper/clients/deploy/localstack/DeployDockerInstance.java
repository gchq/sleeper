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

import org.eclipse.jetty.io.RuntimeIOException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.clients.deploy.localstack.stack.CompactionDockerStack;
import sleeper.clients.deploy.localstack.stack.ConfigurationDockerStack;
import sleeper.clients.deploy.localstack.stack.IngestDockerStack;
import sleeper.clients.deploy.localstack.stack.TableDockerStack;
import sleeper.clients.table.AddTable;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.deploy.PopulateInstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DefaultAsyncCommitBehaviour;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static sleeper.configurationv2.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ENDPOINT_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_QUEUE_WAIT_TIME;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class DeployDockerInstance {
    private final S3Client s3Client;
    private final S3TransferManager s3TransferManager;
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;
    private final Consumer<TableProperties> extraTableProperties;

    private DeployDockerInstance(Builder builder) {
        s3Client = Objects.requireNonNull(builder.s3Client, "s3Client must not be null");
        s3TransferManager = Objects.requireNonNull(builder.s3TransferManager, "s3TransferManager must not be null");
        dynamoClient = Objects.requireNonNull(builder.dynamoClient, "dynamoClient must not be null");
        sqsClient = Objects.requireNonNull(builder.sqsClient, "sqsClient must not be null");
        extraTableProperties = Objects.requireNonNull(builder.extraTableProperties, "extraTableProperties must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        if (System.getenv("AWS_ENDPOINT_URL") == null) {
            throw new IllegalArgumentException("Environment variable AWS_ENDPOINT_URL not set");
        }
        String instanceId = args[0];
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            DeployDockerInstance.builder()
                    .s3Client(s3Client).s3TransferManager(s3TransferManager)
                    .dynamoClient(dynamoClient).sqsClient(sqsClient).build()
                    .deploy(instanceId);
        }
    }

    public void deploy(String instanceId) {
        InstanceProperties instanceProperties = PopulateInstanceProperties.populateDefaultsFromInstanceId(
                new InstanceProperties(), instanceId);
        TableProperties tableProperties = generateTableProperties(instanceProperties);
        extraTableProperties.accept(tableProperties);
        deploy(instanceProperties, List.of(tableProperties));
    }

    public void deploy(InstanceProperties instanceProperties, List<TableProperties> tables) {
        setForcedInstanceProperties(instanceProperties);

        ConfigurationDockerStack.from(instanceProperties, s3Client).deploy();
        TableDockerStack.from(instanceProperties, s3Client, dynamoClient).deploy();
        IngestDockerStack.from(instanceProperties, dynamoClient, sqsClient).deploy();
        CompactionDockerStack.from(instanceProperties, dynamoClient, sqsClient).deploy();

        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

        for (TableProperties tableProperties : tables) {
            try {
                new AddTable(instanceProperties, tableProperties, s3Client, s3TransferManager, dynamoClient).run();
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }

    private static void setForcedInstanceProperties(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        instanceProperties.set(CONFIG_BUCKET, InstanceProperties.getConfigBucketFromInstanceId(instanceId));
        instanceProperties.setEnumList(OPTIONAL_STACKS, OptionalStack.LOCALSTACK_STACKS);
        instanceProperties.set(ACCOUNT, "test-account");
        instanceProperties.set(VPC_ID, "test-vpc");
        instanceProperties.set(SUBNETS, "test-subnet");
        instanceProperties.set(REGION, "us-east-1");
        instanceProperties.set(QUERY_RESULTS_BUCKET, "sleeper-" + instanceId + "-query-results");
        instanceProperties.set(DEFAULT_ASYNC_COMMIT_BEHAVIOUR, DefaultAsyncCommitBehaviour.DISABLED.toString());
        instanceProperties.set(COMPACTION_TASK_WAIT_TIME_IN_SECONDS, "1");
        instanceProperties.set(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, "1");
        instanceProperties.set(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, "0");
        instanceProperties.set(INGEST_JOB_QUEUE_WAIT_TIME, "1");
        instanceProperties.set(ENDPOINT_URL, System.getenv("AWS_ENDPOINT_URL"));
    }

    private static TableProperties generateTableProperties(InstanceProperties instanceProperties) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "system-test");
        tableProperties.setSchema(Schema.builder().rowKeyFields(new Field("key", new StringType())).build());
        return tableProperties;
    }

    public static final class Builder {
        private S3Client s3Client;
        private S3TransferManager s3TransferManager;
        private DynamoDbClient dynamoClient;
        private SqsClient sqsClient;
        private Consumer<TableProperties> extraTableProperties = tableProperties -> {
        };

        private Builder() {
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder s3TransferManager(S3TransferManager s3TransferManager) {
            this.s3TransferManager = s3TransferManager;
            return this;
        }

        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder extraTableProperties(Consumer<TableProperties> extraTableProperties) {
            this.extraTableProperties = extraTableProperties;
            return this;
        }

        public DeployDockerInstance build() {
            return new DeployDockerInstance(this);
        }
    }
}
