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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.testutils.InMemoryTransactionBodyStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator.LatestSnapshotsMetadataLoader;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotSaver.SnapshotMetadataSaver;
import sleeper.statestore.transactionlog.snapshots.TransactionLogSnapshotDeleter.SnapshotFileDeleter;

import java.io.IOException;
import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogSnapshotTestBase extends LocalStackTestBase {
    @TempDir
    private java.nio.file.Path tempDir;
    protected final Schema schema = createSchemaWithKey("key", new LongType());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    public void setup() throws IOException {
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();
    }

    protected void createSnapshots(TableProperties table) {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(table);
        createSnapshots(table, snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    protected void createSnapshots(TableProperties table, LatestSnapshotsMetadataLoader latestSnapshotsLoader) {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(table);
        createSnapshots(table, latestSnapshotsLoader, snapshotStore::saveSnapshot);
    }

    protected void createSnapshots(
            TableProperties table, SnapshotMetadataSaver snapshotSaver) {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = snapshotStore(table);
        createSnapshots(table, snapshotStore::getLatestSnapshots, snapshotSaver);
    }

    protected void createSnapshotsAt(TableProperties table, Instant creationTime) throws Exception {
        DynamoDBTransactionLogSnapshotMetadataStore snapshotStore = new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, table, dynamoClientV2, () -> creationTime);
        createSnapshots(table, snapshotStore::getLatestSnapshots, snapshotStore::saveSnapshot);
    }

    protected void createSnapshots(
            TableProperties table, LatestSnapshotsMetadataLoader latestSnapshotsLoader, SnapshotMetadataSaver snapshotSaver) {
        new DynamoDBTransactionLogSnapshotCreator(
                instanceProperties, table,
                transactionLogs.forTable(table).getFilesLogStore(),
                transactionLogs.forTable(table).getPartitionsLogStore(),
                transactionLogs.getTransactionBodyStore(),
                s3ClientV2, s3TransferManager,
                latestSnapshotsLoader, snapshotSaver)
                .createSnapshot();
    }

    protected SnapshotDeletionTracker deleteSnapshotsAt(TableProperties table, Instant deletionTime) {
        return new TransactionLogSnapshotDeleter(
                instanceProperties, table, dynamoClientV2, s3ClientV2)
                .deleteSnapshots(deletionTime);
    }

    protected SnapshotDeletionTracker deleteSnapshotsAt(TableProperties table, Instant deletionTime, SnapshotFileDeleter fileDeleter) {
        return new TransactionLogSnapshotDeleter(
                instanceProperties, table, dynamoClientV2, fileDeleter)
                .deleteSnapshots(deletionTime);
    }

    protected TransactionLogStateStore createStateStoreWithInMemoryTransactionLog(TableProperties table) {
        TransactionLogStateStore stateStore = transactionLogs.stateStoreBuilder(table).build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    protected InMemoryTransactionBodyStore transactionBodyStore() {
        return transactionLogs.getTransactionBodyStore();
    }

    protected DynamoDBTransactionLogSnapshotMetadataStore snapshotStore(TableProperties table) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, table, dynamoClientV2);
    }

    protected TableProperties createTable(String tableId, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getSimpleName());
        return tableProperties;
    }

    protected TransactionLogSnapshotMetadata filesSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(TransactionLogSnapshotMetadata.getBasePath(instanceProperties, table), transactionNumber);
    }

    protected TransactionLogSnapshotMetadata partitionsSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forPartitions(TransactionLogSnapshotMetadata.getBasePath(instanceProperties, table), transactionNumber);
    }

    protected String filesSnapshotObjectKey(TableProperties table, long transactionNumber) {
        return filesSnapshot(table, transactionNumber).getObjectKey();
    }

    protected String partitionsSnapshotObjectKey(TableProperties table, long transactionNumber) {
        return partitionsSnapshot(table, transactionNumber).getObjectKey();
    }

    protected boolean filesSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        // Will only return object if it exists, otherwise throws an exception.
        // HeadObjectRequest used as light weight request for just metadata
        try {
            s3ClientV2.headObject(HeadObjectRequest.builder()
                    .bucket(instanceProperties.get(DATA_BUCKET))
                    .key(filesSnapshot(table, transactionNumber).getObjectKey())
                    .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    protected boolean partitionsSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        // Will only return object if it exists, otherwise throws an exception.
        // HeadObjectRequest used as light weight request for just metadata
        try {
            s3ClientV2.headObject(HeadObjectRequest.builder()
                    .bucket(instanceProperties.get(DATA_BUCKET))
                    .key(partitionsSnapshot(table, transactionNumber).getObjectKey())
                    .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    protected void deleteFilesSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        s3ClientV2.deleteObject(DeleteObjectRequest.builder()
                .bucket(instanceProperties.get(DATA_BUCKET))
                .key(filesSnapshot(table, transactionNumber).getObjectKey())
                .build());
    }

    protected void deletePartitionsSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        s3ClientV2.deleteObject(DeleteObjectRequest.builder()
                .bucket(instanceProperties.get(DATA_BUCKET))
                .key(partitionsSnapshot(table, transactionNumber).getObjectKey())
                .build());
    }

    protected Stream<String> filesInDataBucket() throws Exception {
        return listObjectKeys(instanceProperties.get(DATA_BUCKET)).stream();
    }
}
