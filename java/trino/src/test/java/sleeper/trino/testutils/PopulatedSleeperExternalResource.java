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
package sleeper.trino.testutils;

import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.localstack.test.SleeperLocalStackContainer;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.trino.SleeperConfig;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.localstack.test.LocalStackAwsV2ClientHelper.buildAwsV2Client;

/**
 * This class is a JUnit plugin which starts a local S3 and DynamoDB within a Docker
 * LocalStackContainer. Sleeper tables are created, with the data files themselves stored in a temporary directory on
 * the local disk.
 * <p>
 * Only use one instance of this class at once. The Parquet readers and writers make use of the Hadoop FileSystem and
 * this caches the S3AFileSystem objects which actually communicate with S3. The cache needs to be reset between
 * different recreations of the localstack container. It would be good to fix this in future.
 */
public class PopulatedSleeperExternalResource implements BeforeAllCallback, AfterAllCallback {
    private final Map<String, String> extraPropertiesForQueryRunner;
    private final List<TableDefinition> tableDefinitions;
    private final SleeperConfig sleeperConfig;
    private final LocalStackContainer localStackContainer = SleeperLocalStackContainer.INSTANCE;
    private final HadoopConfigurationProvider hadoopConfigurationProvider = new HadoopConfigurationProviderForLocalStack();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Configuration configuration = SleeperLocalStackClients.HADOOP_CONF;
    private final S3Client s3Client = SleeperLocalStackClients.S3_CLIENT;
    private final S3AsyncClient s3AsyncClient = SleeperLocalStackClients.S3_ASYNC_CLIENT;
    private final DynamoDbClient dynamoDBClient = SleeperLocalStackClients.DYNAMO_CLIENT;
    private QueryAssertions queryAssertions;

    public PopulatedSleeperExternalResource(List<TableDefinition> tableDefinitions) {
        this(Map.of(), tableDefinitions, new SleeperConfig());
    }

    public PopulatedSleeperExternalResource(
            Map<String, String> extraPropertiesForQueryRunner, List<TableDefinition> tableDefinitions,
            SleeperConfig sleeperConfig) {
        this.extraPropertiesForQueryRunner = requireNonNull(extraPropertiesForQueryRunner);
        this.tableDefinitions = requireNonNull(tableDefinitions);
        this.sleeperConfig = requireNonNull(sleeperConfig);
    }

    private void ingestData(
            InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider,
            TableProperties tableProperties, Iterator<Row> rowIterator) throws Exception {
        IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(UUID.randomUUID().toString()).toString())
                .stateStoreProvider(stateStoreProvider)
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(configuration)
                .s3AsyncClient(s3AsyncClient)
                .build().ingestFromRowIterator(tableProperties, rowIterator);
    }

    private TableProperties createTable(InstanceProperties instanceProperties, TableDefinition tableDefinition) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, tableDefinition.schema);
        tableProperties.set(TABLE_NAME, tableDefinition.tableName);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoDBClient).save(tableProperties);
        return tableProperties;
    }

    private TableProperties getTableProperties(String tableName) {
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
        return tablePropertiesProvider.getByName(tableName);
    }

    public StateStore getStateStore(String tableName) {
        StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient);
        return stateStoreFactory.getStateStore(getTableProperties(tableName));
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        System.out.println("LocalStack endpoint:       " + localStackContainer.getEndpoint().toString());

        sleeperConfig.setLocalWorkingDirectory(createTempDirectory(UUID.randomUUID().toString()).toString());

        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(instanceProperties.get(CONFIG_BUCKET))
                .build());
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(instanceProperties.get(DATA_BUCKET))
                .build());
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();

        this.tableDefinitions.forEach(tableDefinition -> {
            try {
                TableProperties tableProperties = createTable(
                        instanceProperties,
                        tableDefinition);
                StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient);
                StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
                update(stateStore).initialise(new PartitionsFromSplitPoints(tableDefinition.schema, tableDefinition.splitPoints).construct());
                ingestData(instanceProperties, stateStoreProvider, tableProperties, tableDefinition.rowStream.iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("--- My buckets ---");
        List<Bucket> buckets = s3Client.listBuckets().buckets();
        buckets.forEach(System.out::println);
        buckets.forEach(bucket -> {
            System.out.printf("--- Contents of bucket %s ---%n", bucket);
            s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket.name())
                    .build()).contents().forEach(System.out::println);
        });

        sleeperConfig.setConfigBucket(instanceProperties.get(CONFIG_BUCKET));
        DistributedQueryRunner distributedQueryRunner = SleeperQueryRunner.createSleeperQueryRunner(
                extraPropertiesForQueryRunner,
                sleeperConfig,
                buildAwsV2Client(localStackContainer, S3Client.builder()),
                buildAwsV2Client(localStackContainer, S3AsyncClient.builder()),
                buildAwsV2Client(localStackContainer, DynamoDbClient.builder()),
                hadoopConfigurationProvider);
        queryAssertions = new QueryAssertions(distributedQueryRunner);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        this.queryAssertions.close();
        // The Hadoop file system maintains a cache of the file system object to use. The S3AFileSystem object
        // retains the endpoint URL and so the cache needs to be cleared whenever the localstack instance changes.
        try {
            FileSystem.closeAll();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public QueryAssertions getQueryAssertions() {
        return this.queryAssertions;
    }

    public static class TableDefinition {
        public final String tableName;
        public final Schema schema;
        public final List<Object> splitPoints;
        public final Stream<Row> rowStream;

        public TableDefinition(
                String tableName, Schema schema, List<Object> splitPoints, Stream<Row> rowStream) {
            this.tableName = tableName;
            this.schema = schema;
            this.splitPoints = splitPoints;
            this.rowStream = rowStream;
        }
    }
}
