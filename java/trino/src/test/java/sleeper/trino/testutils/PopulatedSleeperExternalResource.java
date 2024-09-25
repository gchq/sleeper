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
package sleeper.trino.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
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
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;

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
    private final LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3)
            .withLogConsumer(outputFrame -> System.out.print("LocalStack log: " + outputFrame.getUtf8String()))
            .withEnv("DEBUG", "1");
    private final HadoopConfigurationProvider hadoopConfigurationProvider = new HadoopConfigurationProviderForLocalStack(localStackContainer);
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private Configuration configuration;

    private AmazonS3 s3Client;
    private S3AsyncClient s3AsyncClient;
    private AmazonDynamoDB dynamoDBClient;
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

    private AmazonDynamoDB createDynamoClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    }

    private AmazonS3 createS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    private S3AsyncClient createS3AsyncClient() {
        return buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    }

    private void ingestData(
            InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider,
            TableProperties tableProperties, Iterator<Record> recordIterator) throws Exception {
        IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(UUID.randomUUID().toString()).toString())
                .stateStoreProvider(stateStoreProvider)
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(configuration)
                .s3AsyncClient(s3AsyncClient)
                .build().ingestFromRecordIterator(tableProperties, recordIterator);
    }

    private TableProperties createTable(InstanceProperties instanceProperties, TableDefinition tableDefinition) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, tableDefinition.schema);
        tableProperties.set(TABLE_NAME, tableDefinition.tableName);
        S3TableProperties.getStore(instanceProperties, s3Client, dynamoDBClient).save(tableProperties);
        return tableProperties;
    }

    private TableProperties getTableProperties(String tableName) {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        return tablePropertiesProvider.getByName(tableName);
    }

    public StateStore getStateStore(String tableName) {
        StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, configuration);
        return stateStoreFactory.getStateStore(getTableProperties(tableName));
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        localStackContainer.start();
        configuration = hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties);
        s3Client = createS3Client();
        s3AsyncClient = createS3AsyncClient();
        dynamoDBClient = createDynamoClient();

        System.out.println("S3 endpoint:       " + localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        System.out.println("DynamoDB endpoint: " + localStackContainer.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString());

        sleeperConfig.setLocalWorkingDirectory(createTempDirectory(UUID.randomUUID().toString()).toString());

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDBClient, instanceProperties);
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();

        this.tableDefinitions.forEach(tableDefinition -> {
            try {
                TableProperties tableProperties = createTable(
                        instanceProperties,
                        tableDefinition);
                StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient, configuration);
                StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
                stateStore.initialise(new PartitionsFromSplitPoints(tableDefinition.schema, tableDefinition.splitPoints).construct());
                ingestData(instanceProperties, stateStoreProvider, tableProperties, tableDefinition.recordStream.iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("--- My buckets ---");
        List<Bucket> buckets = s3Client.listBuckets();
        buckets.forEach(System.out::println);
        buckets.forEach(bucket -> {
            System.out.printf("--- Contents of bucket %s ---%n", bucket);
            s3Client.listObjectsV2(bucket.getName()).getObjectSummaries().forEach(System.out::println);
        });

        sleeperConfig.setConfigBucket(instanceProperties.get(CONFIG_BUCKET));
        DistributedQueryRunner distributedQueryRunner = SleeperQueryRunner.createSleeperQueryRunner(
                extraPropertiesForQueryRunner,
                sleeperConfig,
                s3Client,
                s3AsyncClient,
                dynamoDBClient,
                hadoopConfigurationProvider);
        queryAssertions = new QueryAssertions(distributedQueryRunner);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        this.queryAssertions.close();
        this.s3Client.shutdown();
        this.dynamoDBClient.shutdown();
        this.localStackContainer.stop();

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
        public final Stream<Record> recordStream;

        public TableDefinition(
                String tableName, Schema schema, List<Object> splitPoints, Stream<Record> recordStream) {
            this.tableName = tableName;
            this.schema = schema;
            this.splitPoints = splitPoints;
            this.recordStream = recordStream;
        }
    }
}
