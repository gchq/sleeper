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
package sleeper.splitter.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequestSerDe;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestResult;
import sleeper.splitter.find.SplitPartitionJobDefinition;
import sleeper.splitter.find.SplitPartitionJobDefinitionSerDe;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class SplitPartitionLambdaIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(Service.S3, Service.DYNAMODB, Service.SQS);

    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = schemaWithKey("key", new IntType());
    private final PartitionTree partitionTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final TableProperties tableProperties = createTable(schema, partitionTree);
    private final Configuration conf = getHadoopConfiguration(localStackContainer);
    private final SplitPartitionJobDefinitionSerDe serDe = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider());

    @TempDir
    private Path tempDir;

    @Test
    void shouldCommitToStateStoreDirectly() throws Exception {
        // Given
        List<String> filenames = ingestRecordsGetFilenames(IntStream.rangeClosed(1, 100)
                .mapToObj(i -> new Record(Map.of("key", i))));

        // When
        lambdaWithNewPartitionIds("L", "R").splitPartitionFromJson(
                serDe.toJson(new SplitPartitionJobDefinition(
                        tableProperties.get(TABLE_ID),
                        partitionTree.getRootPartition(), filenames)));

        // Then
        assertThat(stateStore().getAllPartitions()).containsExactlyInAnyOrderElementsOf(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 51)
                        .buildList());
    }

    @Test
    void shouldSendAsyncRequestToStateStoreCommitter() throws Exception {
        // Given
        tableProperties.set(PARTITION_SPLIT_ASYNC_COMMIT, "true");
        S3TableProperties.createStore(instanceProperties, s3, dynamoDB).save(tableProperties);
        List<String> filenames = ingestRecordsGetFilenames(IntStream.rangeClosed(1, 100)
                .mapToObj(i -> new Record(Map.of("key", i))));

        // When
        lambdaWithNewPartitionIds("L", "R").splitPartitionFromJson(
                serDe.toJson(new SplitPartitionJobDefinition(
                        tableProperties.get(TABLE_ID),
                        partitionTree.getRootPartition(), filenames)));

        // Then
        PartitionTree expectedTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 51)
                .buildTree();
        assertThat(stateStore().getAllPartitions()).containsExactlyElementsOf(partitionTree.getAllPartitions());
        assertThat(receiveSplitPartitionCommitMessages()).containsExactly(
                new SplitPartitionCommitRequest(
                        tableProperties.get(TABLE_ID),
                        expectedTree.getRootPartition(),
                        expectedTree.getPartition("L"),
                        expectedTree.getPartition("R")));
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        S3InstanceProperties.saveToS3(s3, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();
        return instanceProperties;
    }

    private String createFifoQueueGetUrl() {
        return sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true"))).getQueueUrl();
    }

    private List<SplitPartitionCommitRequest> receiveSplitPartitionCommitMessages() {
        return receiveCommitMessages().stream()
                .map(message -> new SplitPartitionCommitRequestSerDe(schema).fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<Message> receiveCommitMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    private TableProperties createTable(Schema schema, PartitionTree partitionTree) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.createStore(instanceProperties, s3, dynamoDB).createTable(tableProperties);
        try {
            stateStoreProvider().getStateStore(tableProperties)
                    .initialise(partitionTree.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return tableProperties;
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return S3TableProperties.createProvider(instanceProperties, s3, dynamoDB);
    }

    private StateStore stateStore() {
        return stateStoreProvider().getStateStore(tableProperties);
    }

    private StateStoreProvider stateStoreProvider() {
        return StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, conf);
    }

    private SplitPartitionLambda lambdaWithNewPartitionIds(String... ids) {
        return new SplitPartitionLambda(instanceProperties, conf, s3, dynamoDB, sqs, List.of(ids).iterator()::next);
    }

    private List<String> ingestRecordsGetFilenames(Stream<Record> records) throws Exception {
        IngestResult result = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(tempDir.toString())
                .stateStoreProvider(stateStoreProvider())
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(conf)
                .s3AsyncClient(buildAwsV2Client(localStackContainer, Service.S3, S3AsyncClient.builder()))
                .build().ingestFromRecordIterator(tableProperties, records.iterator());
        return result.getFileReferenceList().stream()
                .map(FileReference::getFilename)
                .collect(toUnmodifiableList());
    }
}
