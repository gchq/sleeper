/*
 * Copyright 2022 Crown Copyright
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
package sleeper.clients.admin;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.console.ConsoleOutput;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class TablePropertyReport {

    private final ConsoleOutput out;
    private final AdminConfigStore store;

    public TablePropertyReport(ConsoleOutput out, AdminConfigStore store) {
        this.out = out;
        this.store = store;
    }

    public void print(String instanceId, String tableName) {
        InstanceProperties instanceProperties = store.loadInstanceProperties(instanceId);
        print(store.tablePropertiesProvider(instanceProperties).getTableProperties(tableName));
    }

    private void print(TableProperties tableProperties) {

        Iterator<Map.Entry<Object, Object>> propertyIterator = tableProperties.getPropertyIterator();
        TreeMap<Object, Object> tablePropertyTreeMap = new TreeMap<>();
        while (propertyIterator.hasNext()) {
            Map.Entry<Object, Object> mapElement = propertyIterator.next();
            tablePropertyTreeMap.put(mapElement.getKey(), mapElement.getValue());
        }
        for (TableProperty tableProperty : TableProperty.values()) {
            if (!tablePropertyTreeMap.containsKey(tableProperty.getPropertyName())) {
                tablePropertyTreeMap.put(tableProperty.getPropertyName(), tableProperties.get(tableProperty));
            }
        }
        out.println("\n\n Table Property Report \n -------------------------");
        for (Map.Entry<Object, Object> entry : tablePropertyTreeMap.entrySet()) {
            out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
