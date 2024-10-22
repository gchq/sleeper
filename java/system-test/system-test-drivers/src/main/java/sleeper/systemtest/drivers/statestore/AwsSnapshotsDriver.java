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
package sleeper.systemtest.drivers.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotStore;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;

import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;

public class AwsSnapshotsDriver implements SnapshotsDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsSnapshotsDriver.class);
    private final SystemTestClients clients;

    public AwsSnapshotsDriver(SystemTestClients clients) {
        this.clients = clients;
    }

    @Override
    public void enableCreation(InstanceProperties instanceProperties) {
        LOGGER.info("Enabling transaction log snapshot creation");
        clients.getCloudWatchEvents().enableRule(request -> request
                .name(instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE)));
    }

    @Override
    public void disableCreation(InstanceProperties instanceProperties) {
        LOGGER.info("Disabling transaction log snapshot creation");
        clients.getCloudWatchEvents().disableRule(request -> request
                .name(instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE)));
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestFilesSnapshot(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return snapshotStore(instanceProperties, tableProperties).loadLatestFilesSnapshotIfAtMinimumTransaction(0);
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestPartitionsSnapshot(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return snapshotStore(instanceProperties, tableProperties).loadLatestPartitionsSnapshotIfAtMinimumTransaction(0);
    }

    private DynamoDBTransactionLogSnapshotStore snapshotStore(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties,
                clients.getDynamoDB(), clients.createHadoopConf(instanceProperties, tableProperties));
    }
}
