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

package sleeper.systemtest.drivers.bulkimport;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;

import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_BULK_IMPORT_JOBS;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;

public class CheckBulkImportRecords {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckBulkImportRecords.class);
    private final StateStore stateStore;
    private final long numberOfWriters;
    private final long numberOfRecordsPerWriter;
    private final long numberOfBulkImportJobs;

    public CheckBulkImportRecords(StateStore stateStore, InstanceProperties properties) {
        this.stateStore = stateStore;
        this.numberOfWriters = properties.getLong(NUMBER_OF_WRITERS);
        this.numberOfRecordsPerWriter = properties.getLong(NUMBER_OF_RECORDS_PER_WRITER);
        this.numberOfBulkImportJobs = properties.getLong(NUMBER_OF_BULK_IMPORT_JOBS);
    }

    public void checkRecords() throws StateStoreException {
        long expectedRecords = numberOfWriters * numberOfRecordsPerWriter * numberOfBulkImportJobs;
        LOGGER.info("Expecting {} records ({} writers, {} records per writer, {} total jobs)",
                expectedRecords, numberOfWriters, numberOfRecordsPerWriter, numberOfBulkImportJobs);
        long recordsInStateStore = stateStore.getActiveFiles().stream()
                .mapToLong(FileInfo::getNumberOfRecords).sum();
        LOGGER.info("Found {} records across all files in table", recordsInStateStore);
        if (expectedRecords != recordsInStateStore) {
            throw new IllegalStateException("Some records failed to ingest successfully");
        }
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, systemTestProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, systemTestProperties);
        StateStore stateStore = stateStoreProvider.getStateStore("system-test", tablePropertiesProvider);

        CheckBulkImportRecords checkBulkImportRecords = new CheckBulkImportRecords(stateStore, systemTestProperties);
        checkBulkImportRecords.checkRecords();
        s3Client.shutdown();
        dynamoDB.shutdown();
    }
}
