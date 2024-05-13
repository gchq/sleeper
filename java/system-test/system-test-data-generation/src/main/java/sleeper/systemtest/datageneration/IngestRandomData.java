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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.AssumeSleeperRole;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.SystemTestIngestMode;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.util.UUID;

import static sleeper.systemtest.configuration.SystemTestIngestMode.BATCHER;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestIngestMode.GENERATE_ONLY;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomData.class);

    private final InstanceProperties instanceProperties;
    private final SystemTestPropertyValues systemTestProperties;
    private final String tableName;
    private final AWSSecurityTokenService stsClient;

    private IngestRandomData(
            InstanceProperties instanceProperties, SystemTestPropertyValues systemTestProperties, String tableName,
            AWSSecurityTokenService stsClient) {
        this.instanceProperties = instanceProperties;
        this.systemTestProperties = systemTestProperties;
        this.tableName = tableName;
        this.stsClient = stsClient;
    }

    public void run() throws IOException {
        Ingester ingester = ingester();
        int numIngests = systemTestProperties.getInt(NUMBER_OF_INGESTS_PER_WRITER);
        for (int i = 1; i <= numIngests; i++) {
            LOGGER.info("Starting ingest {}", i);
            ingester.ingest();
            LOGGER.info("Completed ingest {}", i);
        }
        LOGGER.info("Finished");
    }

    public static void main(String[] args) throws IOException {
        InstanceProperties instanceProperties;
        SystemTestPropertyValues systemTestProperties;
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.defaultClient();
        try {
            if (args.length == 2) {
                SystemTestProperties properties = new SystemTestProperties();
                properties.loadFromS3(s3Client, args[0]);
                instanceProperties = properties;
                systemTestProperties = properties.testPropertiesOnly();
            } else if (args.length == 3) {
                instanceProperties = new InstanceProperties();
                instanceProperties.loadFromS3(s3Client, args[0]);
                systemTestProperties = SystemTestStandaloneProperties.fromS3(s3Client, args[2]);
            } else {
                throw new RuntimeException("Wrong number of arguments detected. Usage: IngestRandomData <S3 bucket> <Table name> <optional system test bucket>");
            }

            new IngestRandomData(instanceProperties, systemTestProperties, args[1], stsClient)
                    .run();
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
            stsClient.shutdown();
        }
    }

    private Ingester ingester() {
        SystemTestIngestMode ingestMode = systemTestProperties.getEnumValue(INGEST_MODE, SystemTestIngestMode.class);
        if (ingestMode == DIRECT) {
            return () -> {
                AssumeSleeperRole assumeRole = AssumeSleeperRole.directIngest(stsClient, instanceProperties);
                AmazonS3 s3Client = assumeRole.v1Client(AmazonS3ClientBuilder.standard());
                AmazonDynamoDB dynamoClient = assumeRole.v1Client(AmazonDynamoDBClientBuilder.standard());
                Configuration conf = assumeRole.setInHadoopForS3A(HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
                try {
                    TableProperties tableProperties = tableProperties(s3Client, dynamoClient);
                    StateStoreProvider stateStoreProvider = new StateStoreProvider(instanceProperties, s3Client, dynamoClient, conf);
                    WriteRandomDataDirect.writeWithIngestFactory(instanceProperties, tableProperties, systemTestProperties, stateStoreProvider);
                } finally {
                    s3Client.shutdown();
                    dynamoClient.shutdown();
                }
            };
        }
        AssumeSleeperRole assumeRole = AssumeSleeperRole.ingestByQueue(stsClient, instanceProperties);
        AmazonS3 s3Client = assumeRole.v1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = assumeRole.v1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            TableProperties tableProperties = tableProperties(s3Client, dynamoClient);
            FileIngester ingestFromFile;
            if (ingestMode == QUEUE) {
                ingestFromFile = (jobId, dir) -> IngestRandomDataViaQueue.sendJob(
                        jobId, dir, instanceProperties, tableProperties, systemTestProperties);
            } else if (ingestMode == BATCHER) {
                ingestFromFile = (jobId, dir) -> IngestRandomDataViaBatcher.sendRequest(dir, instanceProperties, tableProperties);
            } else if (ingestMode == GENERATE_ONLY) {
                ingestFromFile = (jobId, dir) -> LOGGER.debug("Generate data only, no message was sent");
            } else {
                throw new IllegalArgumentException("Unrecognised ingest mode: " + ingestMode);
            }
            return writeRandomFileAnd(tableProperties, ingestFromFile);
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }
    }

    private Ingester writeRandomFileAnd(TableProperties tableProperties, FileIngester ingester) {
        return () -> {
            String jobId = UUID.randomUUID().toString();
            String dir = WriteRandomDataFiles.writeToS3GetDirectory(
                    instanceProperties, tableProperties, systemTestProperties, jobId);
            ingester.ingest(jobId, dir);
        };
    }

    private TableProperties tableProperties(AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        return new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient)
                .getByName(tableName);
    }

    interface Ingester {
        void ingest() throws IOException;
    }

    interface FileIngester {
        void ingest(String jobId, String dir) throws IOException;
    }
}
