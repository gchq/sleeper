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
package sleeper.systemtest.suite.investigate;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.job.execution.JavaCompactionRunner;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.testutils.OnDiskTransactionLogs;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.parquet.record.RecordReadSupport;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreArrowFileStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;
import sleeper.systemtest.dsl.util.SystemTestSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class CheckTransactionLogs {
    public static final Logger LOGGER = LoggerFactory.getLogger(CheckTransactionLogs.class);

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final List<TransactionLogEntryHandle> filesLog;
    private final List<TransactionLogEntryHandle> partitionsLog;

    public CheckTransactionLogs(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            List<TransactionLogEntryHandle> filesLog, List<TransactionLogEntryHandle> partitionsLog) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.filesLog = filesLog;
        this.partitionsLog = partitionsLog;
    }

    public static void main(String[] args) throws Exception {
        String instanceId = Objects.requireNonNull(System.getenv("INSTANCE_ID"), "INSTANCE_ID must be set");
        String tableId = Objects.requireNonNull(System.getenv("TABLE_ID"), "TABLE_ID must be set");
        boolean cacheTransactions = Optional.ofNullable(System.getenv("CACHE_TRANSACTIONS")).map(Boolean::parseBoolean).orElse(true);

        CheckTransactionLogs check = load(instanceId, tableId, cacheTransactions);
        PartitionTree partitions = check.partitionTree();
        LOGGER.info("Compaction commit transactions: {}", check.countCompactionCommitTransactions());
        LOGGER.info("Compaction jobs committed: {}", check.countCompactionJobsCommitted());
        var reports = check.reportCompactionTransactionsChangedRecordCount();
        LOGGER.info("Compaction transactions which changed number of records: {}", reports.size());
        for (var report : reports) {
            LOGGER.info("Transaction {} had {} jobs changing records", report.transactionNumber(), report.jobs().size());
        }
        CompactionChangedRecordCount job = reports.stream().flatMap(report -> report.jobs().stream()).findFirst().orElse(null);

        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();

        for (FileReference file : job.inputFiles()) {
            long actualRecords = countActualRecords(file.getFilename(), hadoopConf);
            LOGGER.info("Counted {} actual records in file {}", actualRecords, file.getFilename());
        }
        LOGGER.info("Job {} had {} input files, {} records before, {} records after",
                job.jobId(), job.inputFiles().size(), job.recordsBefore(), job.recordsAfter());

        Partition partition = partitions.getPartition(job.partitionId());
        LOGGER.info("Partition: {}", partition);

        Path tempDir = Files.createTempDirectory("sleeper-test");
        String outputFile = tempDir.resolve(UUID.randomUUID().toString()).toString();
        CompactionJob compactionJob = job.asCompactionJobToNewFile(tableId, outputFile);
        JavaCompactionRunner compactionRunner = new JavaCompactionRunner(ObjectFactory.noUserJars(), hadoopConf);
        RecordsProcessed processed = compactionRunner.compact(compactionJob, check.tableProperties(), partition);
        long actualOutputRecords = countActualRecords(outputFile, hadoopConf);
        LOGGER.info("Counted {} actual records in compaction output, reported {}", actualOutputRecords, processed);
        LOGGER.info("Compation job: {}", new CompactionJobSerDe().toJson(compactionJob));

        DynamoDBTransactionLogSnapshotMetadataStore metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(check.instanceProperties(), check.tableProperties(), dynamoClient);
        StateStoreArrowFileStore fileStore = new StateStoreArrowFileStore(check.tableProperties(), hadoopConf);
        LatestSnapshots latestSnapshots = metadataStore.getLatestSnapshots();
        StateStorePartitions partitionsSnapshot = latestSnapshots.getPartitionsSnapshot().map(fileStore::loadSnapshot).map(snapshot -> snapshot.<StateStorePartitions>getState()).orElse(null);
        Partition partitionFromSnapshot = partitionsSnapshot.byId(job.partitionId()).orElseThrow();
        LOGGER.info("Partition from snapshot: {}", partitionFromSnapshot);
    }

    private static long countActualRecords(String filename, Configuration hadoopConf) throws IOException {
        var path = new org.apache.hadoop.fs.Path(filename);
        long count = 0;
        try (ParquetReader<Record> reader = ParquetReader.builder(new RecordReadSupport(SystemTestSchema.DEFAULT_SCHEMA), path)
                .withConf(hadoopConf)
                .build()) {
            for (Record record = reader.read(); record != null; record = reader.read()) {
                count++;
            }
        }
        return count;
    }

    public InstanceProperties instanceProperties() {
        return instanceProperties;
    }

    public TableProperties tableProperties() {
        return tableProperties;
    }

    public long totalRecordsAtTransaction(long transactionNumber) {
        return filesStateAtTransaction(transactionNumber)
                .references().mapToLong(FileReference::getNumberOfRecords).sum();
    }

    public long countCompactionCommitTransactions() {
        return filesLog.stream().filter(entry -> entry.isType(TransactionType.REPLACE_FILE_REFERENCES)).count();
    }

    public long countCompactionJobsCommitted() {
        return filesLog.stream().filter(entry -> entry.isType(TransactionType.REPLACE_FILE_REFERENCES))
                .mapToLong(entry -> {
                    ReplaceFileReferencesTransaction transaction = entry.castTransaction();
                    return transaction.getJobs().size();
                }).sum();
    }

    public List<CompactionChangedRecordCountReport> reportCompactionTransactionsChangedRecordCount() {
        return CompactionChangedRecordCountReport.findChanges(filesLog);
    }

    public StateStoreFiles filesStateAtTransaction(long transactionNumber) {
        StateStoreFiles state = new StateStoreFiles();
        for (TransactionLogEntryHandle entry : filesLog) {
            entry.apply(state);
            if (entry.transactionNumber() == transactionNumber) {
                return state;
            }
        }
        throw new IllegalArgumentException("Transaction number not found: " + transactionNumber);
    }

    public PartitionTree partitionTree() {
        StateStorePartitions state = new StateStorePartitions();
        for (TransactionLogEntryHandle entry : partitionsLog) {
            entry.apply(state);
        }
        return new PartitionTree(state.all());
    }

    private static CheckTransactionLogs load(String instanceId, String tableId, boolean cacheTransactions) {
        Path cacheDirectory = OnDiskTransactionLogs.getLocalCacheDirectory(instanceId, tableId);
        if (cacheTransactions && Files.isDirectory(cacheDirectory)) {
            return from(OnDiskTransactionLogs.load(cacheDirectory));
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient).getById(tableId);
        TransactionBodyStore bodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
        TransactionLogStore filesLogStore = DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoClient, s3Client);
        TransactionLogStore partitionsLogStore = DynamoDBTransactionLogStore.forPartitions(instanceProperties, tableProperties, dynamoClient, s3Client);
        if (cacheTransactions) {
            return from(OnDiskTransactionLogs.cacheState(instanceProperties, tableProperties, filesLogStore, partitionsLogStore, bodyStore, cacheDirectory));
        } else {
            return new CheckTransactionLogs(
                    instanceProperties, tableProperties,
                    TransactionLogEntryHandle.load(tableId, filesLogStore, bodyStore),
                    TransactionLogEntryHandle.load(tableId, partitionsLogStore, bodyStore));
        }
    }

    private static CheckTransactionLogs from(OnDiskTransactionLogs cache) {
        return new CheckTransactionLogs(
                cache.getInstanceProperties(), cache.getTableProperties(),
                TransactionLogEntryHandle.load(cache.getFilesLogStore()),
                TransactionLogEntryHandle.load(cache.getPartitionsLogStore()));
    }
}
