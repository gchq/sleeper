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
import sleeper.core.statestore.testutils.OnDiskTransactionLogs;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CheckTransactionLogs {
    public static final Logger LOGGER = LoggerFactory.getLogger(CheckTransactionLogs.class);

    private final List<TransactionLogEntryHandle> filesLog;

    public CheckTransactionLogs(List<TransactionLogEntryHandle> filesLog) {
        this.filesLog = filesLog;
    }

    public static void main(String[] args) throws IOException {
        CheckTransactionLogs check = load();
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

    private static CheckTransactionLogs load() {
        String instanceId = Objects.requireNonNull(System.getenv("INSTANCE_ID"), "INSTANCE_ID must be set");
        String tableId = Objects.requireNonNull(System.getenv("TABLE_ID"), "TABLE_ID must be set");
        boolean cacheTransactions = Optional.ofNullable(System.getenv("CACHE_TRANSACTIONS")).map(Boolean::parseBoolean).orElse(true);
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
        if (cacheTransactions) {
            return from(OnDiskTransactionLogs.cacheState(instanceProperties, tableProperties, filesLogStore, filesLogStore, bodyStore, cacheDirectory));
        } else {
            return new CheckTransactionLogs(TransactionLogEntryHandle.load(tableId, filesLogStore, bodyStore));
        }
    }

    private static CheckTransactionLogs from(OnDiskTransactionLogs cache) {
        return new CheckTransactionLogs(TransactionLogEntryHandle.load(cache.getFilesLogStore()));
    }
}
