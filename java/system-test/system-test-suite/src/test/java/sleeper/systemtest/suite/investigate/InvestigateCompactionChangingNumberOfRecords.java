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
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.parquet.record.RecordReadSupport;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.statestore.StateStoreArrowFileReadStore;
import sleeper.statestore.StateStorePartitionsArrowFormat;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;
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
 * This was an investigation after a failed system test where a compaction changed the number of records in a Sleeper
 * table unexpectedly.
 */
public class InvestigateCompactionChangingNumberOfRecords {
    public static final Logger LOGGER = LoggerFactory.getLogger(InvestigateCompactionChangingNumberOfRecords.class);

    private InvestigateCompactionChangingNumberOfRecords() {
    }

    public static void main(String[] args) throws Exception {
        String instanceId = Objects.requireNonNull(System.getenv("INSTANCE_ID"), "INSTANCE_ID must be set");
        String tableId = Objects.requireNonNull(System.getenv("TABLE_ID"), "TABLE_ID must be set");
        boolean cacheTransactions = Optional.ofNullable(System.getenv("CACHE_TRANSACTIONS")).map(Boolean::parseBoolean).orElse(true);

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create()) {
            CheckTransactionLogs check = CheckTransactionLogs.load(instanceId, tableId, cacheTransactions, s3Client, dynamoClient);
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

}
