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
package sleeper.statestore.committer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import sleeper.common.task.QueueMessageCount;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class MultiThreadedStateStoreCommitterLocalStackIT extends LocalStackTestBase {

    private static final long DEFAULT_TIMEOUT = 60 * 1000;

    private InstanceProperties instanceProperties;
    private TablePropertiesStore tablePropertiesStore;
    private TablePropertiesProvider tablePropertiesProvider;
    private StateStoreProvider stateStoreProvider;
    private final Schema schema = createSchemaWithKey("key", new LongType());
    private StateStoreCommitRequestSender commitRequestSender;
    private String commitQ;

    @BeforeEach
    void setUp() {
        commitQ = createFifoQueueGetUrl();

        instanceProperties = createTestInstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, commitQ);

        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

        stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
        tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        commitRequestSender = new SqsFifoStateStoreCommitRequestSender(instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));

        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
    }

    @Test
    void shouldWorkWhenSQSQueueUrlProvidedAsArg() throws Exception {
        TableProperties table = createTable();
        List<String> expectedFiles = sendAddFilesCommitRequest(table, 5);

        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        MultiThreadedStateStoreCommitter committer = new MultiThreadedStateStoreCommitter(s3Client, dynamoClient, sqsClient, configBucket, commitQ);
        runCommitterUntilQueueEmptyOrTimeout(committer);

        List<String> files = getTableFileNames(table);
        assertThat(files).containsExactlyInAnyOrderElementsOf(expectedFiles);
    }

    @Test
    void shouldFailWhenUnableToGetToSQSQueueUrlProvidedAsArg() {
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        MultiThreadedStateStoreCommitter committer = new MultiThreadedStateStoreCommitter(s3Client, dynamoClient, sqsClient, configBucket, "wrong-url");

        assertThatExceptionOfType(QueueDoesNotExistException.class)
            .isThrownBy(() -> committer.run());
    }

    @Test
    void shouldWorkWhenSQSQueueUrlObtainedFromInstanceProperties() throws Exception {
        TableProperties table = createTable();
        List<String> expectedFiles = sendAddFilesCommitRequest(table, 5);

        runCommitterUntilQueueEmptyOrTimeout();

        List<String> files = getTableFileNames(table);
        assertThat(files).containsExactlyInAnyOrderElementsOf(expectedFiles);
    }

    @Test
    void shouldFailWhenUnableToGetToSQSQueueUrlObtainedFromInstanceProperties() {
        TableProperties table = createTable();
        sendAddFilesCommitRequest(table, 5);

        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, "wrong-url");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);

        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        MultiThreadedStateStoreCommitter committer = new MultiThreadedStateStoreCommitter(s3Client, dynamoClient, sqsClient, configBucket);

        assertThatExceptionOfType(QueueDoesNotExistException.class)
            .isThrownBy(() -> runCommitterUntilQueueEmptyOrTimeout(committer));
    }

    @Test
    void shouldProcessCommitsForMultipleTables() throws Exception {
        TableProperties table1 = createTable();
        TableProperties table2 = createTable();
        TableProperties table3 = createTable();

        List<String> table1ExpectedFiles = sendAddFilesCommitRequest(table1, 5);
        List<String> table2ExpectedFiles = sendAddFilesCommitRequest(table2, 5);
        List<String> table3ExpectedFiles = sendAddFilesCommitRequest(table3, 5);
        table1ExpectedFiles.addAll(sendAddFilesCommitRequest(table1, 5));

        runCommitterUntilQueueEmptyOrTimeout();

        List<String> table1Files = getTableFileNames(table1);
        List<String> table2Files = getTableFileNames(table2);
        List<String> table3Files = getTableFileNames(table3);

        assertThat(table1Files).containsExactlyInAnyOrderElementsOf(table1ExpectedFiles);
        assertThat(table2Files).containsExactlyInAnyOrderElementsOf(table2ExpectedFiles);
        assertThat(table3Files).containsExactlyInAnyOrderElementsOf(table3ExpectedFiles);
    }

    private List<String> getTableFileNames(TableProperties tableProperties) {
        List<FileReference> fileRefs = stateStoreProvider.getStateStore(tableProperties).getFileReferences();
        List<String> fileNames = fileRefs.stream().map(FileReference::getFilename).collect(Collectors.toList());
        return fileNames;
    }

    private List<String> sendAddFilesCommitRequest(TableProperties tableProperties, int fileReferenceCount) {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(stateStore);

        List<FileReference> fileRefs = IntStream.range(0, fileReferenceCount).mapToObj(i -> {
            String fileName = UUID.randomUUID().toString() + ".parquet";
            FileReference fileRef = fileFactory.rootFile(fileName, 100);
            return fileRef;
        }).collect(Collectors.toList());

        AddFilesTransaction txn = AddFilesTransaction.fromReferences(fileRefs);
        commitRequestSender.send(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), txn));

        List<String> fileNames = fileRefs.stream().map(FileReference::getFilename).collect(Collectors.toList());
        return fileNames;
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tablePropertiesStore.createTable(tableProperties);

        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        update(stateStore).initialise(tableProperties);

        return tableProperties;
    }

    private void runCommitterUntilQueueEmptyOrTimeout() throws Exception {
        runCommitterUntilQueueEmptyOrTimeout(DEFAULT_TIMEOUT);
    }

    private void runCommitterUntilQueueEmptyOrTimeout(long timeoutInMillis) throws Exception {
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        MultiThreadedStateStoreCommitter committer = new MultiThreadedStateStoreCommitter(s3Client, dynamoClient, sqsClient, configBucket);
        runCommitterUntilQueueEmptyOrTimeout(committer, timeoutInMillis);
    }

    private void runCommitterUntilQueueEmptyOrTimeout(MultiThreadedStateStoreCommitter committer) throws Exception {
        runCommitterUntilQueueEmptyOrTimeout(committer, DEFAULT_TIMEOUT);
    }

    private void runCommitterUntilQueueEmptyOrTimeout(MultiThreadedStateStoreCommitter committer, long timeoutInMillis) throws Exception {
        long startedAt = System.currentTimeMillis();
        committer.runUntil(() -> getCommitQueueMessageCount() == 0 || System.currentTimeMillis() - startedAt >= timeoutInMillis);
    }

    private int getCommitQueueMessageCount() {
        QueueMessageCount counts = QueueMessageCount
            .withSqsClient(sqsClient)
            .getQueueMessageCount(commitQ);
        int totalMessageCount = counts.getApproximateNumberOfMessages() + counts.getApproximateNumberOfMessagesNotVisible();
        System.err.println("Message Count = " + totalMessageCount);
        return totalMessageCount;
    }

}
