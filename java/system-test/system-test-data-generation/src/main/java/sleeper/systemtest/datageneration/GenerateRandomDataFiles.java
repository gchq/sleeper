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

package sleeper.systemtest.datageneration;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.configuration.SystemTestClusterJob;

import java.io.IOException;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

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
        SystemTestClusterJob job = SystemTestClusterJob.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .recordsPerIngest(numberOfRecords)
                .build();
        WriteRandomDataFiles.writeFilesToDirectory(outputDirectory, new InstanceProperties(),
                tableProperties, WriteRandomData.createRecordIterator(job, tableProperties));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <output-directory> <optional-number-of-records>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        String outputDirectory = args[2];
        long numberOfRecords = 100000;
        if (args.length > 3) {
            numberOfRecords = Long.parseLong(args[3]);
        }

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        try {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName(tableName);

            new GenerateRandomDataFiles(tableProperties, numberOfRecords, outputDirectory)
                    .run();
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
        }
    }
}
