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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;

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
        systemTestProperties.setNumber(NUMBER_OF_RECORDS_PER_WRITER, numberOfRecords);
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
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");


        new GenerateRandomDataFiles(tableProperties, numberOfRecords, outputDirectory)
                .run();
    }
}
