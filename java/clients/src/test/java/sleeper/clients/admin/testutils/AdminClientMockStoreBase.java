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
package sleeper.clients.admin.testutils;

import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.clients.admin.AdminClient;
import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;

import java.util.Collections;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public abstract class AdminClientMockStoreBase extends AdminClientTestBase {

    protected final AdminClientPropertiesStore store = mock(AdminClientPropertiesStore.class);
    protected final TableIndex tableIndex = new InMemoryTableIndex();

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        super.setInstanceProperties(instanceProperties);
        when(store.loadInstanceProperties(instanceProperties.get(ID))).thenReturn(instanceProperties);
    }

    @Override
    public void saveTableProperties(TableProperties tableProperties) {
        when(store.loadTableProperties(instanceProperties, tableProperties.get(TABLE_NAME)))
                .thenReturn(tableProperties);
        tableIndex.create(tableProperties.getStatus());
    }

    @Override
    public void startClient(AdminClientTrackerFactory trackers, QueueMessageCount.Client queueClient) throws InterruptedException {
        new AdminClient(tableIndex, store, trackers,
                editor, out.consoleOut(), in.consoleIn(),
                queueClient, properties -> Collections.emptyMap())
                .start(instanceId);
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, TableStatus... tables) {
        setInstanceTables(instanceProperties, Stream.of(tables));
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, Stream<TableStatus> tables) {
        setInstanceProperties(instanceProperties);
        tables.map(table -> createValidTableProperties(instanceProperties, table))
                .forEach(this::saveTableProperties);
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
