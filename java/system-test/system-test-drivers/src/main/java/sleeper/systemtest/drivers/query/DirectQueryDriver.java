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

package sleeper.systemtest.drivers.query;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.recordretrieval.QueryExecutor;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.dsl.query.QueryDriver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DirectQueryDriver implements QueryDriver {
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    private final SystemTestInstanceContext instance;
    private final SystemTestClients clients;

    public DirectQueryDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.clients = clients;
    }

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        return new QueryAllTablesInParallelDriver(instance, new DirectQueryDriver(instance, clients));
    }

    public List<Record> run(Query query) {
        TableProperties tableProperties = instance.getTablePropertiesByDeployedName(query.getTableName()).orElseThrow();
        StateStore stateStore = instance.getStateStore(tableProperties);
        PartitionTree tree = getPartitionTree(stateStore);
        try (CloseableIterator<Record> recordIterator = executor(tableProperties, stateStore, tree).execute(query)) {
            return stream(recordIterator)
                    .collect(Collectors.toUnmodifiableList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (QueryException e) {
            throw new RuntimeException(e);
        }
    }

    private PartitionTree getPartitionTree(StateStore stateStore) {
        try {
            return new PartitionTree(stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private QueryExecutor executor(TableProperties tableProperties, StateStore stateStore, PartitionTree partitionTree) {
        try {
            QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties, stateStore,
                    new LeafPartitionRecordRetrieverImpl(EXECUTOR_SERVICE, clients.createHadoopConf()));
            executor.init(partitionTree.getAllPartitions(), stateStore.getPartitionToReferencedFilesMap());
            return executor;
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }
}
