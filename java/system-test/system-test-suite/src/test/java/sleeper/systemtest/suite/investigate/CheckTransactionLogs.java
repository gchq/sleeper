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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.testutils.OnDiskTransactionLogs;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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

    public static CheckTransactionLogs load(String instanceId, String tableId, boolean cacheTransactions, S3Client s3Client, DynamoDbClient dynamoClient) {
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
}
