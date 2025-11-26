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

package sleeper.systemtest.drivers.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.foreign.bridge.FFIContext;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.rowretrieval.QueryEngineSelector;
import sleeper.query.core.rowretrieval.QueryExecutor;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;
import sleeper.query.runner.rowretrieval.LeafPartitionRowRetrieverImpl;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.dsl.query.QueryDriver;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DirectQueryDriver implements QueryDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(DirectQueryDriver.class);
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
    private static final BufferAllocator ALLOCATOR = new RootAllocator();
    private static final FFIContext<DataFusionQueryFunctions> CONTEXT = FFIContext
            .getFFIContextSafely(DataFusionQueryFunctions.class);

    private final SystemTestInstanceContext instance;
    private final LeafPartitionRowRetrieverProvider rowRetrieverProvider;

    public DirectQueryDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.rowRetrieverProvider = QueryEngineSelector.javaAndDataFusion(
                new LeafPartitionRowRetrieverImpl.Provider(EXECUTOR_SERVICE,
                        clients.tableHadoopProvider(instance.getInstanceProperties())),
                new DataFusionLeafPartitionRowRetriever.Provider(clients.createDataFusionAwsConfig(), ALLOCATOR,
                        CONTEXT));
    }

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        return new QueryAllTablesInParallelDriver(instance, new DirectQueryDriver(instance, clients));
    }

    public List<Row> run(Query query) {
        TableProperties tableProperties = instance.getTablePropertiesByDeployedName(query.getTableName()).orElseThrow();
        StateStore stateStore = instance.getStateStore(tableProperties);
        PartitionTree tree = getPartitionTree(stateStore);
        try {
            QueryExecutor queryExecutor = executor(tableProperties, stateStore, tree);
            CloseableIterator<Row> rowIterator = queryExecutor.execute(query);
            return stream(rowIterator)
                    .collect(Collectors.toUnmodifiableList());
        } catch (QueryException e) {
            throw new RuntimeException(e);
        }
    }

    private PartitionTree getPartitionTree(StateStore stateStore) {
        return new PartitionTree(stateStore.getAllPartitions());
    }

    private QueryExecutor executor(TableProperties tableProperties, StateStore stateStore,
            PartitionTree partitionTree) {
        LeafPartitionRowRetriever rowRetriever = rowRetrieverProvider.getRowRetriever(tableProperties);
        return new QueryExecutor(
                QueryPlanner.initialiseNow(tableProperties, stateStore),
                new LeafPartitionQueryExecutor(ObjectFactory.noUserJars(), tableProperties, rowRetriever));
    }

    private static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }
}
