/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetrieverProvider;
import sleeper.query.core.rowretrieval.ParQueryExecutor;
import sleeper.query.core.rowretrieval.QueryEngineSelector;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;
import sleeper.query.runner.rowretrieval.LeafPartitionRowRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Allows a user to run a query from the command line. An instance of this class cannot be used concurrently in multiple
 * threads, due to how query executors and state store objects are cached. This may be changed in a future version.
 */
public class ParQueryClient extends QueryCommandLineClient {

    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;
    private final LeafPartitionRowRetrieverProvider rowRetrieverProvider;
    private final Map<String, ParQueryExecutor> cachedQueryExecutors = new HashMap<>();

    public ParQueryClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, ObjectFactory objectFactory, StateStoreProvider stateStoreProvider,
            LeafPartitionRowRetrieverProvider rowRetrieverProvider) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out);
        this.objectFactory = objectFactory;
        this.stateStoreProvider = stateStoreProvider;
        this.rowRetrieverProvider = rowRetrieverProvider;
    }

    @Override
    protected void init(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);

        int threadCount = Integer.parseInt(System.getenv().getOrDefault("TCOUNT", "1"));
        System.out.println("Thread count " + threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(30);
        Configuration hadoopConf = TableHadoopConfigurationProvider.forClient(instanceProperties).getConfiguration(tableProperties);

        List<LeafPartitionQueryExecutor> things = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            BufferAllocator allocator = new RootAllocator();
            FFIContext<DataFusionQueryFunctions> context = FFIContext.getFFIContext(DataFusionQueryFunctions.class);
            // things.add(new LeafPartitionQueryExecutor(objectFactory, tableProperties, new LeafPartitionRowRetrieverImpl(executorService, hadoopConf, tableProperties)));
            things.add(new LeafPartitionQueryExecutor(objectFactory, tableProperties, new DataFusionLeafPartitionRowRetriever(DataFusionAwsConfig.getDefault(instanceProperties), allocator, context)));
        }

        if (!cachedQueryExecutors.containsKey(tableName)) {
            ParQueryExecutor executor = new ParQueryExecutor(QueryPlanner.initialiseNow(tableProperties, stateStoreProvider.getStateStore(tableProperties)), things);
            cachedQueryExecutors.put(tableName, executor);
        }
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        Schema schema = tableProperties.getSchema();

        CloseableIterator<Row> rows;
        Instant startTime = Instant.now();
        try {
            rows = runQuery(query);
        } catch (QueryException e) {
            out.println("Encountered an error while running query " + query.getQueryId());
            e.printStackTrace(out.printStream());
            return;
        }
        out.println("Returned Rows:");
        long count = 0L;
        while (rows.hasNext()) {
            out.println(rows.next().toString(schema));
            count++;
        }

        out.println("Query took " + LoggedDuration.withFullOutput(startTime, Instant.now()) + " to return " + count + " rows");
    }

    private CloseableIterator<Row> runQuery(Query query) throws QueryException {
        return cachedQueryExecutors.get(query.getTableName()).execute(query);
    }

    public static void main(String[] args) throws ObjectFactoryException, InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];

        ExecutorService executorService = Executors.newFixedThreadPool(30);
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder());
                BufferAllocator allocator = new RootAllocator();
                FFIContext<DataFusionQueryFunctions> context = FFIContext.getFFIContext(DataFusionQueryFunctions.class)) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
            new ParQueryClient(
                    instanceProperties,
                    new DynamoDBTableIndex(instanceProperties, dynamoClient),
                    S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient),
                    ConsoleInput.stdIn(), ConsoleOutput.stdOut(),
                    new S3UserJarsLoader(instanceProperties, s3Client).buildObjectFactory(),
                    StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient),
                    QueryEngineSelector.javaAndDataFusion(
                            new LeafPartitionRowRetrieverImpl.Provider(executorService, TableHadoopConfigurationProvider.forClient(instanceProperties)),
                            new DataFusionLeafPartitionRowRetriever.Provider(DataFusionAwsConfig.getDefault(instanceProperties), allocator, context)))
                    .run();
        } finally {
            executorService.shutdown();
        }
    }
}
