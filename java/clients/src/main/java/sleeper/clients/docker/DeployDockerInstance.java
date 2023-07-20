/*
 * Copyright 2022-2023 Crown Copyright
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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.clients.deploy.PopulateInstanceProperties;
import sleeper.clients.deploy.PopulateTableProperties;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStoreCreator;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableCreator;

import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class DeployDockerInstance {
    private DeployDockerInstance() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());

        InstanceProperties instanceProperties = generateInstanceProperties(instanceId);
        TableProperties tableProperties = generateTableProperties(instanceProperties);
        createBuckets(s3Client, instanceProperties, tableProperties);
        instanceProperties.saveToS3(s3Client);
        tableProperties.saveToS3(s3Client);

        new TableCreator(s3Client, dynamoDB, instanceProperties).createTable(tableProperties);
        DynamoDBStateStore stateStore = new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDB).create();
        stateStore.initialise();
        setupIngest(sqsClient, dynamoDB, instanceProperties);
    }

    private static void setupIngest(AmazonSQS sqsClient, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        DynamoDBIngestJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        sqsClient.createQueue(instanceProperties.get(INGEST_JOB_QUEUE_URL));
    }

    private static InstanceProperties generateInstanceProperties(String instanceId) {
        InstanceProperties instanceProperties = PopulateInstanceProperties.populateDefaultsFromInstanceId(
                new InstanceProperties(), instanceId);
        instanceProperties.set(OPTIONAL_STACKS, "IngestStack,CompactionStack,PartitionSplittingStack,QueryStack");
        instanceProperties.set(ACCOUNT, "test-account");
        instanceProperties.set(VPC_ID, "test-vpc");
        instanceProperties.set(SUBNETS, "test-subnet");
        instanceProperties.set(REGION, "us-east-1");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, instanceId + "-IngestJobQ");
        instanceProperties.set(INGEST_SOURCE_BUCKET, "sleeper-" + instanceId + "-ingest-source");
        return instanceProperties;
    }

    private static TableProperties generateTableProperties(InstanceProperties instanceProperties) {
        return PopulateTableProperties.builder()
                .tableName("system-test")
                .instanceProperties(instanceProperties)
                .schema(Schema.builder().rowKeyFields(new Field("key", new StringType())).build())
                .build().populate();
    }

    private static void createBuckets(AmazonS3 s3Client, InstanceProperties instanceProperties, TableProperties tableProperties) {
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3Client.createBucket(instanceProperties.get(INGEST_SOURCE_BUCKET));
        s3Client.createBucket(tableProperties.get(DATA_BUCKET));
    }
}
