/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.clients.admin.AdminClientPropertiesStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStore;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class AdminClientMockStoreBase extends AdminClientTestBase {

    protected final AdminClientPropertiesStore store = mock(AdminClientPropertiesStore.class);

    @Override
    public AdminClientPropertiesStore getStore() {
        return store;
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties) {
        when(store.loadInstanceProperties(instanceProperties.get(ID))).thenReturn(instanceProperties);
    }

    @Override
    public void setInstanceProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        setInstanceProperties(instanceProperties);
        when(store.loadTableProperties(instanceProperties, tableProperties.get(TABLE_NAME)))
                .thenReturn(tableProperties);
    }

    protected void setInstanceTables(InstanceProperties instanceProperties, String... tableNames) {
        setInstanceProperties(instanceProperties);
        when(store.listTables(instanceProperties.get(ID))).thenReturn(Arrays.asList(tableNames));
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
}
