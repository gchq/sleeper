/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class InMemoryAdminClientProperties implements AdminClientPropertiesStore.Client {
    private final Map<String, InstanceProperties> instanceIdToProperties = new HashMap<>();
    private final Map<String, TableIndex> instanceIdToTableIndex = new HashMap<>();
    private final Map<String, TablePropertiesStore> instanceIdToTablePropertiesStore = new HashMap<>();
    private final Map<String, InMemoryTransactionLogsPerTable> instanceIdToTransactionLogs = new HashMap<>();
    private final Map<String, StateStore> tableIdToStateStore = new HashMap<>();
    private final boolean defensiveCopy;
    private InstanceProperties localInstanceProperties;
    private List<TableProperties> localTableProperties = List.of();

    private InMemoryAdminClientProperties(boolean defensiveCopy) {
        this.defensiveCopy = defensiveCopy;
    }

    public static InMemoryAdminClientProperties create() {
        return new InMemoryAdminClientProperties(true);
    }

    public static InMemoryAdminClientProperties createReturningExactInstance() {
        return new InMemoryAdminClientProperties(false);
    }

    public void setInstanceProperties(InstanceProperties instanceProperties) {
        saveInstanceProperties(instanceProperties);
        initInstance(instanceProperties);
    }

    public void setStateStore(TableProperties tableProperties, StateStore stateStore) {
        tableIdToStateStore.put(tableProperties.get(TABLE_ID), stateStore);
    }

    public InstanceProperties getLocalInstanceProperties() {
        return localInstanceProperties;
    }

    public List<TableProperties> getLocalTableProperties() {
        return localTableProperties;
    }

    private void initInstance(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        TableIndex tableIndex = instanceIdToTableIndex.computeIfAbsent(instanceId, id -> new InMemoryTableIndex());
        instanceIdToTablePropertiesStore.computeIfAbsent(instanceId,
                id -> defensiveCopy ? InMemoryTableProperties.getStore(tableIndex)
                        : InMemoryTableProperties.getStoreReturningExactInstance(tableIndex));
        instanceIdToTransactionLogs.computeIfAbsent(instanceId, id -> new InMemoryTransactionLogsPerTable());
    }

    @Override
    public InstanceProperties loadInstancePropertiesNoValidation(String instanceId) {
        return copyIfSet(Objects.requireNonNull(
                instanceIdToProperties.get(instanceId),
                "Instance properties not set for instance " + instanceId));
    }

    @Override
    public void saveInstanceProperties(InstanceProperties instanceProperties) {
        instanceIdToProperties.put(instanceProperties.get(ID), copyIfSet(instanceProperties));
    }

    @Override
    public void saveLocalProperties(InstanceProperties instanceProperties, Stream<TableProperties> tablePropertiesStream) throws IOException {
        localInstanceProperties = copyIfSet(instanceProperties);
        localTableProperties = tablePropertiesStream.map(this::copyIfSet).toList();
    }

    @Override
    public TableIndex createTableIndex(InstanceProperties instanceProperties) {
        return instanceIdToTableIndex.get(instanceProperties.get(ID));
    }

    @Override
    public TablePropertiesStore createTablePropertiesStore(InstanceProperties instanceProperties) {
        return instanceIdToTablePropertiesStore.get(instanceProperties.get(ID));
    }

    @Override
    public StateStore createStateStore(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return Objects.requireNonNull(
                tableIdToStateStore.get(tableProperties.get(TABLE_ID)),
                "State store not set for table " + tableProperties.getStatus());
    }

    private InstanceProperties copyIfSet(InstanceProperties properties) {
        if (defensiveCopy) {
            return InstanceProperties.copyOf(properties);
        } else {
            return properties;
        }
    }

    private TableProperties copyIfSet(TableProperties properties) {
        if (defensiveCopy) {
            return TableProperties.copyOf(properties);
        } else {
            return properties;
        }
    }

}
