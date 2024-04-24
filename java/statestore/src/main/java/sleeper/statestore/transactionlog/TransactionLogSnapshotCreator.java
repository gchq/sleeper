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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStoreFiles;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore.LatestSnapshots;

import java.util.Optional;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;

public class TransactionLogSnapshotCreator {
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration configuration;

    public TransactionLogSnapshotCreator(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            AmazonDynamoDB dynamoDB, Configuration configuration) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.dynamoDB = dynamoDB;
        this.configuration = configuration;
    }

    public void run(InvokeForTableRequest tableRequest) throws StateStoreException {
        tableRequest.getTableIds().stream()
                .map(tablePropertiesProvider::getById)
                .forEach(this::createSnapshot);
    }

    public void createSnapshot(TableProperties table) {
        DynamoDBTransactionLogSnapshotStore snapshotStore = new DynamoDBTransactionLogSnapshotStore(instanceProperties, table, dynamoDB);
        Optional<LatestSnapshots> latestSnapshotsOpt = snapshotStore.getLatestSnapshots();
        StateStore stateStore = stateStoreProvider.getStateStore(table);
        try {
            saveFilesSnapshot(table, stateStore, snapshotStore, latestSnapshotsOpt);
            savePartitionsSnapshot(table, stateStore, snapshotStore, latestSnapshotsOpt);
        } catch (DuplicateSnapshotException | StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveFilesSnapshot(TableProperties table, StateStore stateStore,
            DynamoDBTransactionLogSnapshotStore snapshotStore, Optional<LatestSnapshots> latestSnapshotsOpt) throws DuplicateSnapshotException, StateStoreException {
        long lastTransactionNumber = 0L;
        if (latestSnapshotsOpt.isPresent()) {
            lastTransactionNumber = latestSnapshotsOpt.get().getFilesSnapshot().getTransactionNumber();
        }
        TransactionLogFilesSnapshotSerDe filesSnapshotSerDe = new TransactionLogFilesSnapshotSerDe(configuration);
        String snapshotPath = filesSnapshotSerDe.save(getBasePath(instanceProperties, table), getFilesState(stateStore), lastTransactionNumber);
        snapshotStore.saveFiles(snapshotPath, lastTransactionNumber);
    }

    private void savePartitionsSnapshot(TableProperties table, StateStore stateStore,
            DynamoDBTransactionLogSnapshotStore snapshotStore, Optional<LatestSnapshots> latestSnapshotsOpt) throws DuplicateSnapshotException, StateStoreException {
        long lastTransactionNumber = 0L;
        if (latestSnapshotsOpt.isPresent()) {
            lastTransactionNumber = latestSnapshotsOpt.get().getPartitionsSnapshot().getTransactionNumber();
        }
        TransactionLogPartitionsSnapshotSerDe partitionsSnapshotSerDe = new TransactionLogPartitionsSnapshotSerDe(table.getSchema(), configuration);
        String snapshotPath = partitionsSnapshotSerDe.save(getBasePath(instanceProperties, table), getPartitionsState(stateStore), lastTransactionNumber);
        snapshotStore.savePartitions(snapshotPath, lastTransactionNumber);
    }

    private StateStoreFiles getFilesState(StateStore store) throws StateStoreException {
        StateStoreFiles state = new StateStoreFiles();
        store.getAllFilesWithMaxUnreferenced(100).getFiles().forEach(state::add);
        return state;
    }

    private StateStorePartitions getPartitionsState(StateStore store) throws StateStoreException {
        StateStorePartitions state = new StateStorePartitions();
        store.getAllPartitions().forEach(state::put);
        return state;
    }

    private static String getBasePath(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TableProperty.TABLE_ID);
    }
}
