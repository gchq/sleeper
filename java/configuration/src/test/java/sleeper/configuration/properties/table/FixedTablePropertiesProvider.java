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
package sleeper.configuration.properties.table;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class FixedTablePropertiesProvider extends TablePropertiesProvider {
    public FixedTablePropertiesProvider(TableProperties tableProperties) {
        this(List.of(tableProperties));
    }

    public FixedTablePropertiesProvider(List<TableProperties> tables) {
        super(tableName -> tables.stream()
                .filter(table -> Objects.equals(tableName, table.get(TABLE_NAME)))
                .findFirst().orElseThrow(), Integer.MAX_VALUE, () -> Instant.MIN);
    }
}
