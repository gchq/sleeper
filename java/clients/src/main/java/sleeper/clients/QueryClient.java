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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.QueryException;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.Query;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.utils.HadoopConfigurationProvider.getConfigurationForClient;

/**
 * Allows a user to run a query from the command line.
 */
public class QueryClient extends QueryCommandLineClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryCommandLineClient.class);
    
    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;
    private final ExecutorService executorService;
    private final Map<String, QueryExecutor> cachedQueryExecutors = new HashMap<>();

    public QueryClient(AmazonS3 s3Client, InstanceProperties instanceProperties, AmazonDynamoDB dynamoDBClient, Configuration conf) throws ObjectFactoryException {
        super(s3Client, dynamoDBClient, instanceProperties);
        this.objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
        this.stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, conf);
        this.executorService = Executors.newFixedThreadPool(30);
    }

    @Override
    protected void init(TableProperties tableProperties) throws StateStoreException {
        String tableName = tableProperties.get(TABLE_NAME);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(getInstanceProperties(), tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToActiveFilesMap();
       LOGGER.info("Retrieved " + partitions.size() + " partitions from StateStore");

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
        long startTime = System.currentTimeMillis();
        try {
            records = runQuery(query);
        } catch (QueryException e) {
           LOGGER.info("Encountered an error while running query " + query.getQueryId());
            e.printStackTrace();
            return;
        }
       LOGGER.info("Returned Records:");
        long count = 0L;
        while (records.hasNext()) {
           LOGGER.info(records.next().toString(schema));
            count++;
        }

        double delta = (System.currentTimeMillis() - startTime) / 1000.0;
       LOGGER.info("Query took " + delta + " seconds to return " + count + " records");
    }

    private CloseableIterator<Record> runQuery(Query query) throws QueryException {
        QueryExecutor queryExecutor = cachedQueryExecutors.get(query.getTableName());
        return queryExecutor.execute(query);
    }

    public static void main(String[] args) throws StateStoreException, ObjectFactoryException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 amazonS3 = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        QueryClient queryClient = new QueryClient(amazonS3, instanceProperties, dynamoDB, getConfigurationForClient(instanceProperties));
        queryClient.run();
    }
}
