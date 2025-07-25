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

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;

import java.io.IOException;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class GenerateRandomDataFiles {
    private final TableProperties tableProperties;
    private final long numberOfRows;
    private final String outputDirectory;

    public GenerateRandomDataFiles(TableProperties tableProperties, long numberOfRows, String outputDirectory) {
        this.tableProperties = tableProperties;
        this.numberOfRows = numberOfRows;
        this.outputDirectory = outputDirectory;
    }

    private void run() throws IOException {
        SystemTestDataGenerationJob job = SystemTestDataGenerationJob.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .rowsPerIngest(numberOfRows)
                .build();
        WriteRandomDataFiles.writeFilesToDirectory(outputDirectory, new InstanceProperties(),
                tableProperties, WriteRandomData.createRowIterator(job, tableProperties));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <output-directory> <optional-number-of-rows>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        String outputDirectory = args[2];
        long numberOfRows = 100000;
        if (args.length > 3) {
            numberOfRows = Long.parseLong(args[3]);
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName(tableName);

            new GenerateRandomDataFiles(tableProperties, numberOfRows, outputDirectory)
                    .run();
        }
    }
}
