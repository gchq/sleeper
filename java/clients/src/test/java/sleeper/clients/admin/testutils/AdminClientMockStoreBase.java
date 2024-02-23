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
package sleeper.clients.admin.testutils;

import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.AdminClient;
import sleeper.clients.admin.AdminClientStatusStoreFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.job.common.QueueMessageCount;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class AdminClientMockStoreBase extends AdminClientTestBase {

    protected final AdminClientPropertiesStore store = mock(AdminClientPropertiesStore.class);
    private InstanceProperties instanceProperties;
    protected final TableIndex tableIndex = new InMemoryTableIndex();

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        when(store.loadInstanceProperties(instanceProperties.get(ID))).thenReturn(instanceProperties);
        instanceId = instanceProperties.get(ID);
        this.instanceProperties = instanceProperties;
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        when(store.loadTableProperties(instanceProperties, tableProperties.get(TABLE_NAME)))
                .thenReturn(tableProperties);
        tableIndex.create(tableProperties.getStatus());
    }

    @Override
    public void startClient(AdminClientStatusStoreFactory statusStores, QueueMessageCount.Client queueClient)
            throws InterruptedException {
        new AdminClient(tableIndex, store, statusStores,
                editor, out.consoleOut(), in.consoleIn(),
                queueClient, (properties -> Collections.emptyMap()))
                .start(instanceId);
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, TableStatus... tableIds) {
        setInstanceProperties(instanceProperties);
        List.of(tableIds).forEach(tableIndex::create);
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, List<TableStatus> onlineTableIds, List<TableStatus> offlineTableIds) {
        setInstanceProperties(instanceProperties);
        List<String> allTableNames = Stream.concat(onlineTableIds.stream(), offlineTableIds.stream())
                .map(TableStatus::getTableName)
                .collect(Collectors.toList());
        allTableNames.stream()
                .map(tableName -> TableStatus.uniqueIdAndName(UUID.randomUUID().toString(), tableName))
                .forEach(tableIndex::create);
        offlineTableIds.stream()
                .map(TableStatus::getTableName)
                .map(tableIndex::getTableByName)
                .flatMap(Optional::stream)
                .forEach(tableId -> tableIndex.update(tableId.takeOffline()));
    }

    protected void setTableProperties(String tableName) {
        InstanceProperties properties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(properties, tableName);
        setInstanceProperties(properties, tableProperties);
    }

    protected void setStateStoreForTable(String tableName, StateStore stateStore) {
        InstanceProperties properties = createValidInstanceProperties();
        TableProperties tableProperties = createValidTableProperties(properties, tableName);
        setInstanceProperties(properties);
        when(store.loadTableProperties(properties, tableName))
                .thenReturn(tableProperties);
        when(store.loadStateStore(properties.get(ID), tableProperties))
                .thenReturn(stateStore);
    }

    protected void verifyWithNumberOfPromptsBeforeExit(int numberOfInvocations) {
        InOrder order = Mockito.inOrder(in.mock);
        order.verify(in.mock, times(numberOfInvocations)).promptLine(any());
        order.verify(in.mock).waitForLine();
        order.verify(in.mock).promptLine(any());
        order.verifyNoMoreInteractions();
    }
}
