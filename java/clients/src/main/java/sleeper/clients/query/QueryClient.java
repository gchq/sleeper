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
package sleeper.clients.query;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.recordretrieval.QueryExecutor;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
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
public class QueryClient extends QueryCommandLineClient {

    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;
    private final ExecutorService executorService;
    private final Map<String, QueryExecutor> cachedQueryExecutors = new HashMap<>();

    public QueryClient(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient,
            ConsoleInput in, ConsoleOutput out) throws ObjectFactoryException {
        this(instanceProperties, s3Client, dynamoClient, in, out,
                new S3UserJarsLoader(instanceProperties, s3Client, makeTemporaryDirectory()).buildObjectFactory(),
                StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient));
    }

    public QueryClient(InstanceProperties instanceProperties, S3Client s3Client, DynamoDbClient dynamoClient,
            ConsoleInput in, ConsoleOutput out, ObjectFactory objectFactory, StateStoreProvider stateStoreProvider) {
        this(instanceProperties, new DynamoDBTableIndex(instanceProperties, dynamoClient),
                S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient),
                in, out, objectFactory, stateStoreProvider);
    }

    public QueryClient(InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, ObjectFactory objectFactory, StateStoreProvider stateStoreProvider) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out);
        this.objectFactory = objectFactory;
        this.stateStoreProvider = stateStoreProvider;
        this.executorService = Executors.newFixedThreadPool(30);
    }

    public static Path makeTemporaryDirectory() {
        try {
            Path tempDir = Files.createTempDirectory(null);
            tempDir.toFile().deleteOnExit();
            return tempDir;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void init(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(getInstanceProperties(), tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();
        out.println("Retrieved " + partitions.size() + " partitions from StateStore");

        if (!cachedQueryExecutors.containsKey(tableName)) {
            QueryExecutor queryExecutor = new QueryExecutor(objectFactory, tableProperties, stateStoreProvider.getStateStore(tableProperties),
                    new LeafPartitionRecordRetrieverImpl(executorService, conf, tableProperties));
            queryExecutor.init(partitions, partitionToFileMapping);
            cachedQueryExecutors.put(tableName, queryExecutor);
        }
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        Schema schema = tableProperties.getSchema();

        CloseableIterator<Record> records;
        Instant startTime = Instant.now();
        try {
            records = runQuery(query);
        } catch (QueryException e) {
            out.println("Encountered an error while running query " + query.getQueryId());
            e.printStackTrace(out.printStream());
            return;
        }
        out.println("Returned Records:");
        long count = 0L;
        while (records.hasNext()) {
            out.println(records.next().toString(schema));
            count++;
        }

        out.println("Query took " + LoggedDuration.withFullOutput(startTime, Instant.now()) + " to return " + count + " records");
    }

    private CloseableIterator<Record> runQuery(Query query) throws QueryException {
        QueryExecutor queryExecutor = cachedQueryExecutors.get(query.getTableName());
        return queryExecutor.execute(query);
    }

    public static void main(String[] args) throws ObjectFactoryException, InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            QueryClient queryClient = new QueryClient(
                    instanceProperties, s3Client, dynamoClient,
                    new ConsoleInput(System.console()), new ConsoleOutput(System.out));
            queryClient.run();
        }
    }
}
