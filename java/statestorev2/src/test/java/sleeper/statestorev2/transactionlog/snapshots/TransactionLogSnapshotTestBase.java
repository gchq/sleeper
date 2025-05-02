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
package sleeper.statestorev2.transactionlog.snapshots;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.testutils.InMemoryTransactionBodyStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestorev2.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestorev2.transactionlog.TransactionLogStateStoreCreator;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotCreator.LatestSnapshotsMetadataLoader;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotSaver.SnapshotMetadataSaver;
import sleeper.statestorev2.transactionlog.snapshots.TransactionLogSnapshotDeleter.SnapshotFileDeleter;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotSaver.getBasePath;

public class TransactionLogSnapshotTestBase extends LocalStackTestBase {
    @TempDir
    private java.nio.file.Path tempDir;
    private FileSystem fs;
    protected final Schema schema = createSchemaWithKey("key", new LongType());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();

    @BeforeEach
    public void setup() throws IOException {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();
        fs = FileSystem.get(hadoopConf);
    }

    protected TransactionLogSnapshotMetadata getLatestPartitionsSnapshot(TableProperties table) {
        return snapshotStore(table).getLatestSnapshots().getPartitionsSnapshot().orElseThrow();
    }

    protected TransactionLogSnapshotMetadata getLatestFilesSnapshot(TableProperties table) {
        return snapshotStore(table).getLatestSnapshots().getFilesSnapshot().orElseThrow();
    }

    protected void deleteSnapshotFile(TransactionLogSnapshotMetadata snapshot) throws Exception {
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(snapshot.getPath());
        FileSystem fs = path.getFileSystem(hadoopConf);
        fs.delete(path, false);
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
                hadoopConf, latestSnapshotsLoader, snapshotSaver)
                .createSnapshot();
    }

    protected SnapshotDeletionTracker deleteSnapshotsAt(TableProperties table, Instant deletionTime) {
        return new TransactionLogSnapshotDeleter(
                instanceProperties, table, dynamoClientV2, hadoopConf)
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
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName());
        return tableProperties;
    }

    protected TransactionLogSnapshotMetadata filesSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(getBasePath(instanceProperties, table), transactionNumber);
    }

    protected TransactionLogSnapshotMetadata partitionsSnapshot(TableProperties table, long transactionNumber) {
        return TransactionLogSnapshotMetadata.forPartitions(getBasePath(instanceProperties, table), transactionNumber);
    }

    protected String filesSnapshotPath(TableProperties table, long transactionNumber) {
        return filesSnapshot(table, transactionNumber).getPath();
    }

    protected String partitionsSnapshotPath(TableProperties table, long transactionNumber) {
        return partitionsSnapshot(table, transactionNumber).getPath();
    }

    protected boolean filesSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        return fs.exists(new Path(filesSnapshot(table, transactionNumber).getPath()));
    }

    protected boolean partitionsSnapshotFileExists(TableProperties table, long transactionNumber) throws IOException {
        return fs.exists(new Path(partitionsSnapshot(table, transactionNumber).getPath()));
    }

    protected void deleteFilesSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        fs.delete(new Path(filesSnapshot(table, transactionNumber).getPath()), false);
    }

    protected void deletePartitionsSnapshotFile(TableProperties table, long transactionNumber) throws Exception {
        fs.delete(new org.apache.hadoop.fs.Path(partitionsSnapshot(table, transactionNumber).getPath()), false);
    }

    protected Stream<String> tableFiles(TableProperties tableProperties) throws Exception {
        Path tableFilesPath = new Path(getBasePath(instanceProperties, tableProperties));
        if (!fs.exists(tableFilesPath)) {
            return Stream.empty();
        }
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(tableFilesPath, true);
        List<String> files = new ArrayList<>();
        while (iterator.hasNext()) {
            files.add(iterator.next().getPath().toUri().toString());
        }
        return files.stream();
    }
}
