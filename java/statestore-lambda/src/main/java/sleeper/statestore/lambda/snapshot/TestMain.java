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
package sleeper.statestore.lambda.snapshot;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.LoadLocalProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotCreator;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.table.TableFilePaths;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreArrowFileStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static sleeper.core.properties.table.TableProperty.PARTITIONS_SNAPSHOT_BATCH_SIZE;

/**
 * Test.
 */
public class TestMain {
    public static final Logger LOGGER = LoggerFactory.getLogger(TestMain.class);

    private TestMain() {
    }

    public static void main(String[] args) throws Exception {
        Path configDir = Paths.get(Objects.requireNonNull(System.getenv("CONFIG_DIR"), "CONFIG_DIR must be set"));
        String snapshotFile = Objects.requireNonNull(System.getenv("SNAPSHOT_FILE"), "SNAPSHOT_FILE must be set");
        InstanceProperties instanceProperties = LoadLocalProperties.loadInstancePropertiesFromDirectory(configDir);
        TableProperties tableProperties = LoadLocalProperties.loadTablesFromDirectory(instanceProperties, configDir).findFirst().orElseThrow();

        Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        StateStoreArrowFileStore fileStore = new StateStoreArrowFileStore(tableProperties, hadoopConf);
        LOGGER.info("Loading from file: {}", snapshotFile);
        List<Partition> partitions = fileStore.loadPartitions(snapshotFile);
        LOGGER.info("Loaded partitions: {}", partitions);
        String partitionId = partitions.stream().findFirst().orElseThrow().getId();

        TableFilePaths paths = TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties);
        String outputFile = paths.getFilePathPrefix() + "/test/" + UUID.randomUUID().toString();
        // Path tempDir = Files.createTempDirectory("sleeper-test");
        // String outputFile = tempDir.resolve(UUID.randomUUID().toString()).toString();

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        TransactionLogStore partitionLogStore = DynamoDBTransactionLogStore.forPartitions(
                instanceProperties, tableProperties, dynamoClient, s3Client);
        TransactionBodyStore bodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
        TransactionLogSnapshot newSnapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                TransactionLogSnapshot.partitionsInitialState(), partitionLogStore, bodyStore, PartitionTransaction.class, tableProperties.getStatus())
                .orElseThrow();
        StateStorePartitions snapshotState = newSnapshot.getState();
        Partition computedSnapshotPartition = snapshotState.byId(partitionId).orElseThrow();
        LOGGER.info("Computed snapshot partition: {}", computedSnapshotPartition);
        LOGGER.info("Saving computed snapshot to {}", outputFile);
        tableProperties.set(PARTITIONS_SNAPSHOT_BATCH_SIZE, "20000");
        fileStore.savePartitions(outputFile, snapshotState.all());
        List<Partition> found2 = fileStore.loadPartitions(outputFile);
        Partition loadedSnapshotPartition = found2.stream().filter(partition -> partitionId.equals(partition.getId())).findFirst().orElseThrow();
        LOGGER.info("Loaded snapshot partition: {}", loadedSnapshotPartition);
    }

}
