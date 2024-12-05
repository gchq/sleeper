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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class InMemorySleeperInstanceDriver implements SleeperInstanceDriver {

    private final InMemorySleeperTablesDriver tablesDriver;
    private final Map<String, InstanceProperties> instancePropertiesById = new TreeMap<>();

    public InMemorySleeperInstanceDriver(InMemorySleeperTablesDriver tablesDriver) {
        this.tablesDriver = tablesDriver;
    }

    @Override
    public void loadInstanceProperties(InstanceProperties instanceProperties, String instanceId) {
        instanceProperties.resetAndValidate(instancePropertiesById.get(instanceId).getProperties());
    }

    @Override
    public void saveInstanceProperties(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        if (!instancePropertiesById.containsKey(instanceId)) {
            throw new IllegalArgumentException("Instance not found: " + instanceId);
        }
        instancePropertiesById.put(instanceId, InstanceProperties.copyOf(instanceProperties));
    }

    @Override
    public boolean deployInstanceIfNotPresent(String instanceId, DeployInstanceConfiguration deployConfig) {
        if (instancePropertiesById.containsKey(instanceId)) {
            return false;
        }
        InstanceProperties properties = InstanceProperties.copyOf(deployConfig.getInstanceProperties());
        properties.set(ID, instanceId);
        instancePropertiesById.put(instanceId, properties);
        deployConfig.getTableProperties().forEach(table -> tablesDriver.addTable(properties, table));
        return true;
    }

    @Override
    public void redeploy(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        saveInstanceProperties(instanceProperties);
    }

    @Override
    public void resetAfterFirstConnect(InstanceProperties instanceProperties) {
    }
}
