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

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
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
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitPartitionTransaction;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestFactory;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.splitter.core.find.SplitPartitionJobDefinition;
import sleeper.splitter.core.find.SplitPartitionJobDefinitionSerDe;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class SplitPartitionLambdaIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = schemaWithKey("key", new IntType());
    private final PartitionTree partitionTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final TableProperties tableProperties = createTable(schema, partitionTree);
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
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
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
                StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                        new SplitPartitionTransaction(expectedTree.getRootPartition(),
                                List.of(expectedTree.getPartition("L"), expectedTree.getPartition("R")))));
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        return instanceProperties;
    }

    private List<StateStoreCommitRequest> receiveSplitPartitionCommitMessages() {
        return receiveCommitMessages().stream()
                .map(message -> new StateStoreCommitRequestSerDe(tableProperties).fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<Message> receiveCommitMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqsClient.receiveMessage(receiveMessageRequest).getMessages();
    }

    private TableProperties createTable(Schema schema, PartitionTree partitionTree) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).createTable(tableProperties);
        update(stateStoreProvider().getStateStore(tableProperties))
                .initialise(partitionTree.getAllPartitions());
        return tableProperties;
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
    }

    private StateStore stateStore() {
        return stateStoreProvider().getStateStore(tableProperties);
    }

    private StateStoreProvider stateStoreProvider() {
        return StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, hadoopConf);
    }

    private SplitPartitionLambda lambdaWithNewPartitionIds(String... ids) {
        return new SplitPartitionLambda(instanceProperties, hadoopConf, s3Client, dynamoClient, sqsClient, List.of(ids).iterator()::next);
    }

    private List<String> ingestRecordsGetFilenames(Stream<Record> records) throws Exception {
        IngestResult result = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(tempDir.toString())
                .stateStoreProvider(stateStoreProvider())
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(hadoopConf)
                .s3AsyncClient(s3AsyncClient)
                .build().ingestFromRecordIterator(tableProperties, records.iterator());
        return result.getFileReferenceList().stream()
                .map(FileReference::getFilename)
                .collect(toUnmodifiableList());
    }
}
