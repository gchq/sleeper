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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.SplitPartitionTransaction;
import sleeper.core.table.TableStatus;
import sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class TransactionLogTransactionDeleterIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    protected static AmazonDynamoDB dynamoDBClient;
    protected static AmazonS3 s3Client;
    private final Schema schema = schemaWithKey("key", new StringType());
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final Configuration configuration = HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer);
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final TableStatus tableStatus = tableProperties.getStatus();
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("root");
    private TransactionLogStore filesLogStore;
    private TransactionLogStore partitionsLogStore;
    private StateStore stateStore;
    private LatestSnapshots latestSnapshots = LatestSnapshots.empty();

    @BeforeEach
    public void setup() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();
        filesLogStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME), instanceProperties, tableProperties,
                dynamoDBClient, s3Client);
        partitionsLogStore = new DynamoDBTransactionLogStore(
                instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME), instanceProperties, tableProperties,
                dynamoDBClient, s3Client);
        stateStore = TransactionLogStateStore.builder()
                .sleeperTable(tableStatus).schema(schema)
                .filesLogStore(filesLogStore).partitionsLogStore(partitionsLogStore)
                .build();
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
    }

    @Test
    void shouldDeleteOldTransactionWhenTwoAreBeforeLatestSnapshot() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we have a snapshot at the head of the file log
        setLatestFilesSnapshotAt(2, Instant.parse("2024-06-24T15:46:30Z"));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE, 1);

        // When
        deleteOldTransactions();

        // Then
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldNotDeleteOldTransactionWhenFarEnoughBehindButNotOldEnough() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:45Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we have a snapshot at the head of the file log
        setLatestFilesSnapshotAt(2, Instant.parse("2024-06-24T15:46:30Z"));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE, 1);

        // When
        deleteOldTransactions();

        // Then
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(
                        new TransactionLogEntry(1, Instant.parse("2024-06-24T15:45:45Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))),
                        new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldNotDeleteOldTransactionWhenOldEnoughButNotFarEnoughBehind() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we have a snapshot at the head of the file log
        setLatestFilesSnapshotAt(2, Instant.parse("2024-06-24T15:46:30Z"));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 2);
        tableProperties.setNumber(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE, 1);

        // When
        deleteOldTransactions();

        // Then
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(
                        new TransactionLogEntry(1, Instant.parse("2024-06-24T15:45:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))),
                        new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldNotDeleteTransactionsWhenNoSnapshotExistsYet() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        setupAtTime(Instant.parse("2024-06-24T15:45:45Z"), () -> stateStore.addFile(file1));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.addFile(file2));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE, 1);

        // When
        deleteOldTransactions();

        // Then
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(
                        new TransactionLogEntry(1, Instant.parse("2024-06-24T15:45:45Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))),
                        new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    @Test
    void shouldDeleteOldPartitionTransactionWhenTwoAreBeforeLatestSnapshot() throws Exception {
        // Given we have two partitions transactions
        PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("root");
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.initialise(partitions.buildList()));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> partitions
                .splitToNewChildren("root", "L", "R", "m")
                .applySplit(stateStore, "root"));
        // And we have a snapshot at the head of the partitions log
        setLatestPartitionsSnapshotAt(2, Instant.parse("2024-06-24T15:46:30Z"));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE, 1);

        // When
        deleteOldTransactions();

        // Then
        PartitionTree partitionTree = partitions.buildTree();
        assertThat(partitionsLogStore.readTransactionsAfter(0))
                .containsExactly(new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new SplitPartitionTransaction(partitionTree.getRootPartition(), List.of(
                                partitionTree.getPartition("L"),
                                partitionTree.getPartition("R")))));
    }

    @Test
    @Disabled("TODO")
    void shouldDeleteOldTransactionStoredInS3() throws Exception {
        // Given we have two partition transactions, with the first being too large to fit in DynamoDB
        List<String> leafIds = IntStream.range(0, 1000)
                .mapToObj(i -> "" + i)
                .collect(Collectors.toList());
        List<Object> splitPoints = LongStream.range(1, 1000)
                .mapToObj(i -> "split" + i)
                .collect(Collectors.toList());
        setupAtTime(Instant.parse("2024-06-24T15:45:00Z"), () -> stateStore.initialise(
                new PartitionsBuilder(schema).rootFirst("root")
                        .leavesWithSplits(leafIds, splitPoints)
                        .anyTreeJoiningAllLeaves().buildList()));
        setupAtTime(Instant.parse("2024-06-24T15:46:00Z"), () -> stateStore.initialise(
                new PartitionsBuilder(schema).rootFirst("root").buildList()));
        // And we have a snapshot at the head of the partitions log
        setLatestPartitionsSnapshotAt(2, Instant.parse("2024-06-24T15:46:30Z"));
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 1);
        tableProperties.setNumber(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE, 1);

        // When
        deleteOldTransactions();

        // Then
        assertThat(partitionsLogStore.readTransactionsAfter(0))
                .containsExactly(new TransactionLogEntry(2, Instant.parse("2024-06-24T15:46:00Z"),
                        new InitialisePartitionsTransaction(partitions.buildList())));
        assertThat(streamFilesInS3()).isEmpty();
    }

    private Stream<String> streamFilesInS3() {
        return s3Client.listObjects(new ListObjectsRequest()
                .withBucketName(instanceProperties.get(DATA_BUCKET))
                .withPrefix(tableProperties.get(TABLE_ID)))
                .getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey);
    }

    private void setupAtTime(Instant time, SetupFunction setup) throws Exception {
        stateStore.fixFileUpdateTime(time);
        stateStore.fixPartitionUpdateTime(time);
        setup.run();
    }

    private void setLatestFilesSnapshotAt(int transactionNumber, Instant createdTime) {
        latestSnapshots = new LatestSnapshots(TransactionLogSnapshotMetadata.forFiles("", transactionNumber, createdTime), null);
    }

    private void setLatestPartitionsSnapshotAt(int transactionNumber, Instant createdTime) {
        latestSnapshots = new LatestSnapshots(null, TransactionLogSnapshotMetadata.forPartitions("", transactionNumber, createdTime));
    }

    private void deleteOldTransactions() {
        new TransactionLogTransactionDeleter(tableProperties)
                .deleteWithLatestSnapshots(filesLogStore, partitionsLogStore, latestSnapshots);
    }

    /**
     * A setup function.
     */
    public interface SetupFunction {

        /**
         * Performs the setup.
         *
         * @throws Exception if something fails
         */
        void run() throws Exception;
    }
}
