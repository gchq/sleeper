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
import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.io.RuntimeIOException;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.clients.docker.stack.CompactionDockerStack;
import sleeper.clients.docker.stack.ConfigurationDockerStack;
import sleeper.clients.docker.stack.IngestDockerStack;
import sleeper.clients.docker.stack.TableDockerStack;
import sleeper.clients.status.update.AddTable;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.deploy.PopulateInstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.validation.DefaultAsyncCommitBehaviour;
import sleeper.core.properties.validation.OptionalStack;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_ASYNC_COMMIT_BEHAVIOUR;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

public class DeployDockerInstance {
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDB;
    private final SqsClient sqsClient;
    private final Configuration configuration;
    private final Consumer<TableProperties> extraTableProperties;

    private DeployDockerInstance(Builder builder) {
        s3Client = Objects.requireNonNull(builder.s3Client, "s3Client must not be null");
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        sqsClient = Objects.requireNonNull(builder.sqsClient, "sqsClient must not be null");
        configuration = Objects.requireNonNull(builder.configuration, "configuration must not be null");
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
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            DeployDockerInstance.builder().s3Client(s3Client).dynamoDB(dynamoDB).sqsClient(sqsClient)
                    .configuration(getConfigurationForClient()).build()
                    .deploy(instanceId);
        } finally {
            s3Client.shutdown();
            dynamoDB.shutdown();
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
        TableDockerStack.from(instanceProperties, s3Client, dynamoDB).deploy();
        IngestDockerStack.from(instanceProperties, dynamoDB, sqsClient).deploy();
        CompactionDockerStack.from(instanceProperties, dynamoDB, sqsClient).deploy();

        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

        for (TableProperties tableProperties : tables) {
            try {
                new AddTable(s3Client, dynamoDB, instanceProperties, tableProperties, configuration).run();
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
    }

    private static TableProperties generateTableProperties(InstanceProperties instanceProperties) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "system-test");
        tableProperties.setSchema(Schema.builder().rowKeyFields(new Field("key", new StringType())).build());
        return tableProperties;
    }

    public static final class Builder {
        private AmazonS3 s3Client;
        private AmazonDynamoDB dynamoDB;
        private SqsClient sqsClient;
        private Configuration configuration;
        private Consumer<TableProperties> extraTableProperties = tableProperties -> {
        };

        private Builder() {
        }

        public Builder s3Client(AmazonS3 s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder sqsClient(SqsClient sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder configuration(Configuration configuration) {
            this.configuration = configuration;
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
