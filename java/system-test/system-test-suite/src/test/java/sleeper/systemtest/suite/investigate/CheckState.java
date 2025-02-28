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

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.testutils.OnDiskTransactionLogs;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.parquet.record.RecordReadSupport;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;
import sleeper.systemtest.dsl.util.SystemTestSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.toCollection;

public class CheckState {
    public static final Logger LOGGER = LoggerFactory.getLogger(CheckState.class);

    private final List<Entry> filesLog;

    private CheckState(List<Entry> filesLog) {
        this.filesLog = filesLog;
    }

    public static void main(String[] args) throws IOException {
        CheckState check = load();
        LOGGER.info("Compaction commit transactions: {}", check.countCompactionCommitTransactions());
        LOGGER.info("Compaction jobs committed: {}", check.countCompactionJobsCommitted());
        var reports = check.reportCompactionTransactionsChangedRecordCount();
        LOGGER.info("Compaction transactions which changed number of records: {}", reports.size());
        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForClient();
        for (var report : reports) {
            LOGGER.info("Transaction {} had {} jobs changing records", report.transactionNumber(), report.jobs().size());
            for (var job : report.jobs()) {
                for (FileReference file : job.inputFiles()) {
                    long actualRecords = countActualRecords(file, hadoopConf);
                    LOGGER.info("Counted {} actual records in file {}", actualRecords, file.getFilename());
                }
                LOGGER.info("Job {} had {} input files, {} records before, {} records after",
                        job.jobId(), job.inputFiles().size(), job.recordsBefore(), job.recordsAfter());
            }
        }
    }

    private static long countActualRecords(FileReference file, Configuration hadoopConf) throws IOException {
        var path = new org.apache.hadoop.fs.Path(file.getFilename());
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

    public static CheckState load(String tableId, TransactionLogStore logStore, TransactionBodyStore bodyStore) {
        List<Entry> log = logStore
                .readTransactions(TransactionLogRange.fromMinimum(1))
                .map(entry -> {
                    StateStoreTransaction<?> transaction = entry.getTransactionOrLoadFromPointer(tableId, bodyStore);
                    return new Entry(entry, transaction);
                })
                .collect(toCollection(LinkedList::new));
        return new CheckState(log);
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
        StateStoreFiles state = new StateStoreFiles();
        List<CompactionChangedRecordCountReport> reports = new ArrayList<>();
        for (Entry entry : filesLog) {
            if (entry.isType(TransactionType.REPLACE_FILE_REFERENCES)) {
                ReplaceFileReferencesTransaction transaction = entry.castTransaction();
                List<CompactionChangedRecordCount> changes = new ArrayList<>();
                for (ReplaceFileReferencesRequest job : transaction.getJobs()) {
                    String partitionId = job.getPartitionId();
                    List<FileReference> inputFiles = job.getInputFiles().stream()
                            .map(filename -> state.file(filename).orElseThrow()
                                    .getReferenceForPartitionId(partitionId).orElseThrow())
                            .toList();
                    FileReference outputFile = job.getNewReference();
                    if (outputFile.getNumberOfRecords() != inputFiles.stream().mapToLong(FileReference::getNumberOfRecords).sum()) {
                        FileReference outputFileAfter = outputFile.toBuilder().lastStateStoreUpdateTime(entry.original().getUpdateTime()).build();
                        changes.add(new CompactionChangedRecordCount(job, inputFiles, outputFileAfter));
                    }
                }
                if (!changes.isEmpty()) {
                    reports.add(new CompactionChangedRecordCountReport(entry.original(), entry.castTransaction(), changes));
                }
            }
            entry.apply(state);
        }
        return reports;
    }

    public StateStoreFiles filesStateAtTransaction(long transactionNumber) {
        StateStoreFiles state = new StateStoreFiles();
        for (Entry entry : filesLog) {
            entry.apply(state);
            if (entry.transactionNumber() == transactionNumber) {
                return state;
            }
        }
        throw new IllegalArgumentException("Transaction number not found: " + transactionNumber);
    }

    public record CompactionChangedRecordCountReport(TransactionLogEntry entry, ReplaceFileReferencesTransaction transaction, List<CompactionChangedRecordCount> jobs) {
        public long transactionNumber() {
            return entry.getTransactionNumber();
        }
    }

    public record CompactionChangedRecordCount(ReplaceFileReferencesRequest job, List<FileReference> inputFiles, FileReference outputFile) {
        public String jobId() {
            return job.getJobId();
        }

        public long recordsBefore() {
            return inputFiles.stream().mapToLong(FileReference::getNumberOfRecords).sum();
        }

        public long recordsAfter() {
            return outputFile.getNumberOfRecords();
        }
    }

    private record Entry(TransactionLogEntry original, StateStoreTransaction<?> transaction) {

        public <S> void apply(S state) {
            StateStoreTransaction<S> transaction = castTransaction();
            transaction.apply(state, original.getUpdateTime());
        }

        public boolean isType(TransactionType transactionType) {
            return original.getTransactionType() == transactionType;
        }

        public <S, T extends StateStoreTransaction<S>> T castTransaction() {
            return (T) transaction;
        }

        public long transactionNumber() {
            return original.getTransactionNumber();
        }
    }

    private static CheckState load() {
        String instanceId = Objects.requireNonNull(System.getenv("INSTANCE_ID"), "INSTANCE_ID must be set");
        String tableId = Objects.requireNonNull(System.getenv("TABLE_ID"), "TABLE_ID must be set");
        boolean cacheTransactions = Optional.ofNullable(System.getenv("CACHE_TRANSACTIONS")).map(Boolean::parseBoolean).orElse(true);
        Path cacheDirectory = OnDiskTransactionLogs.getLocalCacheDirectory(instanceId, tableId);
        if (cacheTransactions && Files.isDirectory(cacheDirectory)) {
            OnDiskTransactionLogs cache = OnDiskTransactionLogs.load(cacheDirectory);
            return load(tableId, cache.getFilesLogStore(), null);
        }
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient).getById(tableId);
        TransactionBodyStore bodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
        TransactionLogStore filesLogStore = DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoClient, s3Client);
        if (cacheTransactions) {
            OnDiskTransactionLogs cache = OnDiskTransactionLogs.cacheState(instanceProperties, tableProperties, filesLogStore, filesLogStore, bodyStore, cacheDirectory);
            filesLogStore = cache.getFilesLogStore();
        }
        return load(tableId, filesLogStore, bodyStore);
    }
}
