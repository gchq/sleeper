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

package sleeper.clients.status.update;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNameAlreadyExistsException;
import sleeper.core.table.TableNotFoundException;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class RenameTable {
    private final TableIndex tableIndex;
    private final TablePropertiesStore tablePropertiesStore;

    public RenameTable(TableIndex tableIndex, TablePropertiesStore tablePropertiesStore) {
        this.tableIndex = tableIndex;
        this.tablePropertiesStore = tablePropertiesStore;
    }

    public void rename(String oldName, String newName) {
        if (tableIndex.getTableByName(newName).isPresent()) {
            throw new TableNameAlreadyExistsException(newName);
        }
        rename(tableIndex.getTableByName(oldName)
                .orElseThrow(() -> TableNotFoundException.withTableName(oldName)), newName);
    }

    public void rename(TableIdentity oldIdentity, String newName) {
        tableIndex.update(TableIdentity.uniqueIdAndName(oldIdentity.getTableUniqueId(), newName));
        TableProperties tableProperties = tablePropertiesStore.loadProperties(oldIdentity);
        tableProperties.set(TABLE_NAME, newName);
        tablePropertiesStore.save(tableProperties);
    }
}
