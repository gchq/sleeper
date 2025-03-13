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
package sleeper.statestore.transactionlog.snapshots;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.statestore.StateStoreArrowFileStore;
import sleeper.statestore.transactionlog.DuplicateSnapshotException;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator.LatestSnapshotsMetadataLoader;

import java.io.IOException;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;

/**
 * Stores snapshots derived from a transaction log. Holds an index of snapshots in DynamoDB, and stores snapshot data in
 * S3.
 */
public class DynamoDBTransactionLogSnapshotSaver {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotSaver.class);

    private final SnapshotMetadataSaver metadataSaver;
    private final StateStoreArrowFileStore fileStore;
    private final Configuration configuration;
    private final String basePath;

    public DynamoDBTransactionLogSnapshotSaver(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo, Configuration configuration) {
        this(new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, tableProperties, dynamo),
                instanceProperties, tableProperties, configuration);
    }

    private DynamoDBTransactionLogSnapshotSaver(
            DynamoDBTransactionLogSnapshotMetadataStore metadataStore,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this(metadataStore::getLatestSnapshots, metadataStore::saveSnapshot, instanceProperties, tableProperties, configuration);
    }

    DynamoDBTransactionLogSnapshotSaver(
            LatestSnapshotsMetadataLoader latestMetadataLoader, SnapshotMetadataSaver metadataSaver,
            InstanceProperties instanceProperties, TableProperties tableProperties, Configuration configuration) {
        this.metadataSaver = metadataSaver;
        this.fileStore = new StateStoreArrowFileStore(tableProperties, configuration);
        this.configuration = configuration;
        this.basePath = getBasePath(instanceProperties, tableProperties);
    }

    /**
     * Saves a snapshot of files to S3 and to the index. Deletes the file if the metadata fails to save in the index.
     *
     * @param  snapshot                   the snapshot
     * @throws IOException                if the snapshot fails to save to S3
     * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
     */
    public void saveFilesSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forFiles(
                basePath, snapshot.getTransactionNumber());
        StateStoreFiles state = snapshot.getState();
        fileStore.saveFiles(metadata.getPath(), state);
        saveMetadata(metadata);
    }

    /**
     * Saves a snapshot of partitions to S3 and to the index. Deletes the file if the metadata fails to save in the
     * index.
     *
     * @param  snapshot                   the snapshot
     * @throws IOException                if the snapshot fails to save to S3
     * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
     */
    public void savePartitionsSnapshot(TransactionLogSnapshot snapshot) throws IOException, DuplicateSnapshotException {
        TransactionLogSnapshotMetadata metadata = TransactionLogSnapshotMetadata.forPartitions(
                basePath, snapshot.getTransactionNumber());
        StateStorePartitions state = snapshot.getState();
        fileStore.savePartitions(metadata.getPath(), state.all());
        saveMetadata(metadata);
    }

    private void saveMetadata(TransactionLogSnapshotMetadata metadata) throws IOException, DuplicateSnapshotException {
        try {
            metadataSaver.save(metadata);
        } catch (DuplicateSnapshotException | RuntimeException e) {
            LOGGER.info("Failed to save snapshot metadata to DynamoDB. Deleting snapshot file.");
            Path path = new Path(metadata.getPath());
            FileSystem fs = path.getFileSystem(configuration);
            fs.delete(path, false);
            throw e;
        }
    }

    /**
     * Constructs the base path to a table data bucket.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @return                    the full path to the table data bucket (including the file system)
     */
    public static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }

    /**
     * Saves the metadata of a snapshot in the index.
     */
    public interface SnapshotMetadataSaver {

        /**
         * Saves the snapshot metadata.
         *
         * @param  snapshot                   the metadata
         * @throws DuplicateSnapshotException if there is already a snapshot for the given transaction number
         */
        void save(TransactionLogSnapshotMetadata snapshot) throws DuplicateSnapshotException;
    }
}
