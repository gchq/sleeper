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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QueryException;
import sleeper.query.runner.recordretrieval.QueryExecutor;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

/**
 * Allows a user to run a query from the command line. An instance of this class cannot be used concurrently in multiple
 * threads, due to how query executors and state store objects are cached. This may be changed in a future version.
 */
public class QueryClient extends QueryCommandLineClient {

    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;
    private final ExecutorService executorService;
    private final Map<String, QueryExecutor> cachedQueryExecutors = new HashMap<>();

    public QueryClient(AmazonS3 s3Client, InstanceProperties instanceProperties, AmazonDynamoDB dynamoDBClient, Configuration conf,
            ConsoleInput in, ConsoleOutput out) throws ObjectFactoryException {
        this(s3Client, instanceProperties, dynamoDBClient, in, out,
                new ObjectFactory(instanceProperties, s3Client, "/tmp"),
                StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient, conf));
    }

    public QueryClient(AmazonS3 s3Client, InstanceProperties instanceProperties, AmazonDynamoDB dynamoDBClient,
            ConsoleInput in, ConsoleOutput out, ObjectFactory objectFactory, StateStoreProvider stateStoreProvider) {
        this(instanceProperties, new DynamoDBTableIndex(instanceProperties, dynamoDBClient),
                new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient),
                in, out, objectFactory, stateStoreProvider);
    }

    public QueryClient(InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, ObjectFactory objectFactory, StateStoreProvider stateStoreProvider) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out);
        this.objectFactory = objectFactory;
        this.stateStoreProvider = stateStoreProvider;
        this.executorService = Executors.newFixedThreadPool(30);
    }

    @Override
    protected void init(TableProperties tableProperties) throws StateStoreException {
        String tableName = tableProperties.get(TABLE_NAME);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(getInstanceProperties(), tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();
        out.println("Retrieved " + partitions.size() + " partitions from StateStore");

        if (!cachedQueryExecutors.containsKey(tableName)) {
            QueryExecutor queryExecutor = new QueryExecutor(objectFactory, tableProperties, stateStoreProvider.getStateStore(tableProperties),
                    conf, executorService);
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

    public static void main(String[] args) throws StateStoreException, ObjectFactoryException, InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(s3Client, args[0]);
            QueryClient queryClient = new QueryClient(
                    s3Client, instanceProperties, dynamoDBClient,
                    getConfigurationForClient(instanceProperties),
                    new ConsoleInput(System.console()), new ConsoleOutput(System.out));
            queryClient.run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
        }
    }
}
