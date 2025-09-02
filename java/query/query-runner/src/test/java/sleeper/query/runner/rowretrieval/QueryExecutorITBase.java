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
package sleeper.query.runner.rowretrieval;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.rowretrieval.QueryExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;

public class QueryExecutorITBase {
    protected static ExecutorService executorService;

    @TempDir
    public Path folder;

    @BeforeAll
    public static void initExecutorService() {
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterAll
    public static void shutdownExecutorService() {
        executorService.shutdown();
    }

    protected StateStore initialiseStateStore(TableProperties tableProperties, List<Partition> partitions) {
        return InMemoryTransactionLogStateStore.createAndInitialiseWithPartitions(partitions, tableProperties, new InMemoryTransactionLogs());
    }

    protected QueryExecutor queryExecutor(TableProperties tableProperties, StateStore stateStore) {
        return new QueryExecutor(ObjectFactory.noUserJars(),
                tableProperties, stateStore,
                new QueryEngineSelector(executorService, new Configuration()).getRowRetriever(tableProperties));
    }

    protected Query queryWithRegion(Region region) {
        return Query.builder()
                .tableName("myTable")
                .queryId("id")
                .regions(List.of(region))
                .build();
    }

    protected Schema getLongKeySchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    protected void ingestData(InstanceProperties instanceProperties, StateStore stateStore,
            TableProperties tableProperties, Iterator<Row> rowIterator) throws IOException, IteratorCreationException {
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(folder, null).toString())
                .instanceProperties(instanceProperties)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .hadoopConfiguration(new Configuration())
                .build();
        factory.ingestFromRowIterator(tableProperties, rowIterator);
    }

    protected List<Row> getRows() {
        List<Row> rows = new ArrayList<>();
        Row row = new Row();
        row.put("key", 1L);
        row.put("value1", 10L);
        row.put("value2", 100L);
        rows.add(row);
        return rows;
    }

    protected InstanceProperties createInstanceProperties() throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, createTempDirectory(folder, null).toString());
        return instanceProperties;
    }
}
