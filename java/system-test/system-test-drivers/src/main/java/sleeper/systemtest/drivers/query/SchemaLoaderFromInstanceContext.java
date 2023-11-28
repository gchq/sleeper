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

package sleeper.systemtest.drivers.query;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.query.model.QuerySerDe;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.Optional;

public class SchemaLoaderFromInstanceContext implements QuerySerDe.SchemaLoader {

    private final SleeperInstanceContext instance;

    public SchemaLoaderFromInstanceContext(SleeperInstanceContext instance) {
        this.instance = instance;
    }

    @Override
    public Optional<Schema> getSchemaByTableName(String tableName) {
        return instance.getTablePropertiesByName(tableName)
                .map(TableProperties::getSchema);
    }

    @Override
    public Optional<Schema> getSchemaByTableId(String tableId) {
        throw new UnsupportedOperationException("Unexpected lookup by internal table ID in system test: " + tableId);
    }
}
