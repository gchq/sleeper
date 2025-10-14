/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.rowretrieval.InMemoryLeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.QueryExecutor;
import sleeper.query.core.rowretrieval.QueryExecutorNew;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.dsl.query.QueryDriver;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class InMemoryDirectQueryDriver implements QueryDriver {

    private final SystemTestInstanceContext instance;
    private final InMemoryRowStore dataStore;

    InMemoryDirectQueryDriver(SystemTestInstanceContext instance, InMemoryRowStore dataStore) {
        this.instance = instance;
        this.dataStore = dataStore;
    }

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance, InMemoryRowStore dataStore) {
        return new QueryAllTablesInParallelDriver(instance, new InMemoryDirectQueryDriver(instance, dataStore));
    }

    @Override
    public List<Row> run(Query query) {
        TableProperties tableProperties = instance.getTablePropertiesByDeployedName(query.getTableName()).orElseThrow();
        StateStore stateStore = instance.getStateStore(tableProperties);
        LeafPartitionRowRetriever rowRetriever = new InMemoryLeafPartitionRowRetriever(dataStore);
        QueryExecutor planner = new QueryExecutor(ObjectFactory.noUserJars(), stateStore, tableProperties, rowRetriever, Instant.now());
        planner.init();
        QueryExecutorNew executor = new QueryExecutorNew(planner, new LeafPartitionQueryExecutor(ObjectFactory.noUserJars(), tableProperties, rowRetriever));
        try (CloseableIterator<Row> iterator = executor.execute(query)) {
            List<Row> rows = new ArrayList<>();
            iterator.forEachRemaining(rows::add);
            return rows;
        } catch (IOException | QueryException e) {
            throw new RuntimeException(e);
        }
    }
}
