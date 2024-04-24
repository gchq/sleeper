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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomData.class);

    private IngestRandomData() {
    }

    public static void main(String[] args) throws IOException {
        InstanceProperties instanceProperties;
        SystemTestPropertyValues systemTestProperties;
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
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
            TableProperties tableProperties = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient)
                    .getByName(args[1]);

            SystemTestIngestMode ingestMode = systemTestProperties.getEnumValue(INGEST_MODE, SystemTestIngestMode.class);
            if (ingestMode == DIRECT) {
                StateStoreProvider stateStoreProvider = new StateStoreProvider(instanceProperties, s3Client,
                        dynamoClient, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
                WriteRandomDataDirect.writeWithIngestFactory(instanceProperties, tableProperties, systemTestProperties, stateStoreProvider);
            } else {
                String jobId = UUID.randomUUID().toString();
                String dir = WriteRandomDataFiles.writeToS3GetDirectory(
                        instanceProperties, tableProperties, systemTestProperties, jobId);
                if (ingestMode == QUEUE) {
                    IngestRandomDataViaQueue.sendJob(
                            jobId, dir, instanceProperties, tableProperties, systemTestProperties);
                } else if (ingestMode == BATCHER) {
                    IngestRandomDataViaBatcher.sendRequest(dir, instanceProperties, tableProperties);
                } else if (ingestMode == GENERATE_ONLY) {
                    LOGGER.debug("Generate data only, no message was sent");
                } else {
                    throw new IllegalArgumentException("Unrecognised ingest mode: " + ingestMode);
                }
            }
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }
    }
}
