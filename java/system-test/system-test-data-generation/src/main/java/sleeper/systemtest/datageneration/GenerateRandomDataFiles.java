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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;

public class GenerateRandomDataFiles {
    private final TableProperties tableProperties;
    private final long numberOfRecords;
    private final String outputDirectory;

    public GenerateRandomDataFiles(TableProperties tableProperties, long numberOfRecords, String outputDirectory) {
        this.tableProperties = tableProperties;
        this.numberOfRecords = numberOfRecords;
        this.outputDirectory = outputDirectory;
    }

    private void run() throws IOException {
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, numberOfRecords);
        WriteRandomDataFiles.writeFilesToDirectory(outputDirectory, systemTestProperties,
                tableProperties, WriteRandomData.createRecordIterator(systemTestProperties, tableProperties));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <instance-id> <output-directory> <optional-number-of-records>");
        }
        String instanceId = args[0];
        String outputDirectory = args[1];
        long numberOfRecords = 100000;
        if (args.length > 2 && !"".equals(args[2])) {
            numberOfRecords = Long.parseLong(args[2]);
        }

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.getStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");

            new GenerateRandomDataFiles(tableProperties, numberOfRecords, outputDirectory)
                    .run();
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }
    }
}
