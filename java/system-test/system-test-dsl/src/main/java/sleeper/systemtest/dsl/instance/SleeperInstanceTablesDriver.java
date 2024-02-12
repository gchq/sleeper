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

package sleeper.systemtest.dsl.instance;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableIndex;
import sleeper.statestore.StateStoreProvider;

public interface SleeperInstanceTablesDriver {

    void saveTableProperties(InstanceProperties instanceProperties, TableProperties deployedProperties);

    void deleteAllTables(InstanceProperties instanceProperties);

    void addTable(InstanceProperties instanceProperties, TableProperties properties);

    TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties);

    StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties);

    TableIndex tableIndex(InstanceProperties instanceProperties);
}
