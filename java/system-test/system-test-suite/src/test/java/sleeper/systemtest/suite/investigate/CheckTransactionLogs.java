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
package sleeper.systemtest.suite.investigate;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.job.execution.JavaCompactionRunner;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.iterator.IteratorCreationException;
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
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.statestorev2.StateStoreArrowFileReadStore;
import sleeper.statestorev2.StateStorePartitionsArrowFormat;
import sleeper.statestorev2.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestorev2.transactionlog.S3TransactionBodyStore;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestorev2.transactionlog.snapshots.LatestSnapshots;
import sleeper.systemtest.dsl.util.SystemTestSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Checks transaction logs to aid with debugging. This is intended to be used after a failed system test to investigate
 * the state of the system. This will cache transaction logs locally to enable repeated runs to investigate the state,
 * as you change the code in the main method.
 */
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

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create()) {
            CheckTransactionLogs check = load(instanceId, tableId, cacheTransactions, s3Client, dynamoClient);
            examine(check, s3Client, dynamoClient);
        }
    }

    private static void examine(CheckTransactionLogs check, S3Client s3Client, DynamoDbClient dynamoClient) throws IOException, IteratorCreationException {
        PartitionTree partitions = check.partitionTree();
        LOGGER.info("Compaction commit transactions: {}", check.countCompactionCommitTransactions());
        LOGGER.info("Compaction jobs committed: {}", check.countCompactionJobsCommitted());
        var reports = check.reportCompactionTransactionsChangedRecordCount();
        LOGGER.info("Compaction transactions which changed number of records: {}", reports.size());
        for (var report : reports) {
            LOGGER.info("Transaction {} had {} jobs changing records", report.transactionNumber(), report.jobs().size());
        }
        CompactionChangedRecordCount job = reports.stream()
                .flatMap(report -> report.jobs().stream())
                .findFirst().orElse(null);
        if (job == null) {
            LOGGER.info("Found no compaction changing the number of records.");
            return;
        }

        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForClient();

        for (FileReference file : job.inputFiles()) {
            long actualRecords = countActualRecords(file.getFilename(), hadoopConf);
            LOGGER.info("Counted {} actual records in file {}", actualRecords, file.getFilename());
        }
        LOGGER.info("Job {} had {} input files, {} records before, {} records after",
                job.jobId(), job.inputFiles().size(), job.recordsBefore(), job.recordsAfter());

        Partition partition = partitions.getPartition(job.partitionId());
        LOGGER.info("Partition: {}", partition);

        // Rerun compaction to a local file & count actual records
        Path tempDir = Files.createTempDirectory("sleeper-test");
        String outputFile = tempDir.resolve(UUID.randomUUID().toString()).toString();
        CompactionJob compactionJob = job.asCompactionJobToNewFile(check.tableProperties().get(TABLE_ID), outputFile);
        JavaCompactionRunner compactionRunner = new JavaCompactionRunner(ObjectFactory.noUserJars(), hadoopConf, new LocalFileSystemSketchesStore());
        RecordsProcessed processed = compactionRunner.compact(compactionJob, check.tableProperties(), partition);
        long actualOutputRecords = countActualRecords(outputFile, hadoopConf);
        LOGGER.info("Counted {} actual records in compaction output, reported {}", actualOutputRecords, processed);
        LOGGER.info("Compation job: {}", new CompactionJobSerDe().toJson(compactionJob));

        DynamoDBTransactionLogSnapshotMetadataStore metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(check.instanceProperties(), check.tableProperties(), dynamoClient);
        StateStoreArrowFileReadStore snapshotReadStore = new StateStoreArrowFileReadStore(check.instanceProperties(), s3Client);
        LatestSnapshots latestSnapshots = metadataStore.getLatestSnapshots();
        StateStorePartitions partitionsSnapshot = latestSnapshots.getPartitionsSnapshot().map(snapshotReadStore::loadSnapshot).map(snapshot -> snapshot.<StateStorePartitions>getState()).orElse(null);
        Partition partitionFromSnapshot = partitionsSnapshot.byId(job.partitionId()).orElseThrow();
        LOGGER.info("Partition from snapshot: {}", partitionFromSnapshot);

        Path snapshotFile = tempDir.resolve(UUID.randomUUID().toString());
        try (BufferAllocator allocator = new RootAllocator();
                OutputStream out = Files.newOutputStream(snapshotFile);
                WritableByteChannel channel = Channels.newChannel(out)) {
            StateStorePartitionsArrowFormat.write(List.of(partition), allocator, channel, 1000);
        }
        try (BufferAllocator allocator = new RootAllocator();
                InputStream in = Files.newInputStream(snapshotFile);
                ReadableByteChannel channel = Channels.newChannel(in)) {
            Partition partitionFromLocalFile = StateStorePartitionsArrowFormat.read(allocator, channel).partitions().stream().findFirst().orElse(null);
            LOGGER.info("Partition from local file: {}", partitionFromLocalFile);
        }
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

    private static CheckTransactionLogs load(String instanceId, String tableId, boolean cacheTransactions, S3Client s3Client, DynamoDbClient dynamoClient) {
        Path cacheDirectory = OnDiskTransactionLogs.getLocalCacheDirectory(instanceId, tableId);
        if (cacheTransactions && Files.isDirectory(cacheDirectory)) {
            return from(OnDiskTransactionLogs.load(cacheDirectory));
        }
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
