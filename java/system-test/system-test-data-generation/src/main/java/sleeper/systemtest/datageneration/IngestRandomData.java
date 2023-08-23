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
package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.IngestMode;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestPropertyValues;
import sleeper.systemtest.configuration.SystemTestsProperties;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;

import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;

/**
 * Entrypoint for SystemTest image. Writes random data to Sleeper using the mechanism (ingestMode) defined in
 * the properties which were written to S3.
 */
public class IngestRandomData {

    private IngestRandomData() {
    }

    public static void main(String[] args) throws IOException, ObjectFactoryException {
        InstanceProperties instanceProperties;
        SystemTestPropertyValues systemTestProperties;
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        if (args.length == 2) {
            SystemTestProperties properties = new SystemTestProperties();
            properties.loadFromS3(s3Client, args[0]);
            instanceProperties = properties;
            systemTestProperties = properties.testPropertiesOnly();
        } else if (args.length == 3) {
            instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3(s3Client, args[0]);
            SystemTestsProperties properties = new SystemTestsProperties();
            properties.loadFromS3(s3Client, args[2]);
            systemTestProperties = properties;
        } else {
            throw new RuntimeException("Wrong number of arguments detected. Usage: IngestRandomData <S3 bucket> <Table name> <optional system test bucket>");
        }
        TableProperties tableProperties = new TablePropertiesProvider(s3Client, instanceProperties)
                .getTableProperties(args[1]);

        s3Client.shutdown();

        String ingestMode = systemTestProperties.get(INGEST_MODE);
        if (IngestMode.QUEUE.name().equalsIgnoreCase(ingestMode) || IngestMode.BULK_IMPORT_QUEUE.name().equalsIgnoreCase(ingestMode)) {
            WriteRandomDataViaQueue.writeAndSendToQueue(ingestMode, instanceProperties, tableProperties, systemTestProperties);
        } else if (IngestMode.DIRECT.name().equalsIgnoreCase(ingestMode)) {
            StateStoreProvider stateStoreProvider = new StateStoreProvider(AmazonDynamoDBClientBuilder.defaultClient(),
                    instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
            WriteRandomDataDirect.writeWithIngestFactory(instanceProperties, tableProperties, systemTestProperties, stateStoreProvider);
        } else if (IngestMode.GENERATE_ONLY.name().equalsIgnoreCase(ingestMode)) {
            WriteRandomDataFiles.writeToS3GetDirectory(
                    instanceProperties, tableProperties,
                    WriteRandomData.createRecordIterator(systemTestProperties, tableProperties));
        } else {
            throw new IllegalArgumentException("Unrecognised ingest mode: " + ingestMode +
                    ". Only direct and queue ingest modes are available.");
        }
    }
}
