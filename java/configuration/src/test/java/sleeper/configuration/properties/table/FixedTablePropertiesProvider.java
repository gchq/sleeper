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
package sleeper.configuration.properties.table;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

/**
 * A fake table properties provider that holds the table properties in memory.
 */
public class FixedTablePropertiesProvider extends TablePropertiesProvider {
    public FixedTablePropertiesProvider(TableProperties tableProperties) {
        this(List.of(tableProperties));
    }

    public FixedTablePropertiesProvider(Collection<TableProperties> tables) {
        super(InMemoryTableProperties.getStoreReturningExactInstances(tables),
                Duration.ofMinutes(Integer.MAX_VALUE), () -> Instant.MIN);
    }
}
