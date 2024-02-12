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

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.model.Query;
import sleeper.query.model.QueryException;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.query.runner.recordretrieval.QueryExecutor;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.dsl.query.QueryDriver;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class InMemoryQueryDriver implements QueryDriver {

    private final SleeperInstanceContext instance;
    private final InMemoryDataStore dataStore;

    private InMemoryQueryDriver(SleeperInstanceContext instance, InMemoryDataStore dataStore) {
        this.instance = instance;
        this.dataStore = dataStore;
    }

    public static QueryAllTablesDriver allTablesDriver(SleeperInstanceContext instance, InMemoryDataStore dataStore) {
        return new QueryAllTablesInParallelDriver(instance, new InMemoryQueryDriver(instance, dataStore));
    }

    @Override
    public List<Record> run(Query query) {
        TableProperties tableProperties = instance.getTablePropertiesByName(query.getTableName()).orElseThrow();
        StateStore stateStore = instance.getStateStore(tableProperties);
        QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(), stateStore, tableProperties, dataStore, Instant.now());
        try {
            executor.init();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        try (CloseableIterator<Record> iterator = executor.execute(query)) {
            List<Record> records = new ArrayList<>();
            iterator.forEachRemaining(records::add);
            return records;
        } catch (IOException | QueryException e) {
            throw new RuntimeException(e);
        }
    }
}
