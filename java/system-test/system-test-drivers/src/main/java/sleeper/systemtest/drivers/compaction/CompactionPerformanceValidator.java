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
package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;

public class CompactionPerformanceValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionPerformanceValidator.class);
    private final int numberOfJobsExpected;
    private final long numberOfRecordsExpected;
    private final double minRecordsPerSecond;

    private CompactionPerformanceValidator(Builder builder) {
        numberOfJobsExpected = builder.numberOfJobsExpected;
        numberOfRecordsExpected = builder.numberOfRecordsExpected;
        minRecordsPerSecond = builder.minRecordsPerSecond;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (args.length != 5) {
            throw new IllegalArgumentException("Usage: " +
                    "<instance ID> <table name> <expected jobs> <expected records> <min records per second>");
        }

        String instanceId = args[0];
        String tableName = args[1];
        CompactionPerformanceValidator validator = builder()
                .numberOfJobsExpected(Integer.parseInt(args[2]))
                .numberOfRecordsExpected(Long.parseLong(args[3]))
                .minRecordsPerSecond(Double.parseDouble(args[4]))
                .build();

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        TableProperties tableProperties = new TableProperties(systemTestProperties);
        tableProperties.loadFromS3(s3Client, tableName);
        s3Client.shutdown();

        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        StateStore stateStore = new StateStoreFactory(dynamoDB, systemTestProperties, new Configuration())
                .getStateStore(tableProperties);
        CompactionJobStatusStore statusStore = CompactionJobStatusStoreFactory
                .getStatusStore(dynamoDB, systemTestProperties);
        CompactionPerformanceResults results = CompactionPerformanceResults.loadActual(
                tableProperties, stateStore, statusStore);
        dynamoDB.shutdown();

        validator.test(results);
        LOGGER.info("Compaction performance validation passed");
    }

    public void test(CompactionPerformanceResults results) {
        if (results.getNumberOfJobs() != numberOfJobsExpected) {
            throw new IllegalStateException("Actual number of compaction jobs " + results.getNumberOfJobs() +
                    " did not match expected value " + numberOfJobsExpected);
        }
        if (results.getNumberOfRecords() != numberOfRecordsExpected) {
            throw new IllegalStateException("Actual number of records " + results.getNumberOfRecords() +
                    " did not match expected value " + numberOfRecordsExpected);
        }
        if (results.getWriteRate() < minRecordsPerSecond) {
            throw new IllegalStateException(String.format(
                    "Records per second rate of %.2f was slower than expected %.2f",
                    results.getWriteRate(), minRecordsPerSecond));
        }
    }

    public static final class Builder {
        private int numberOfJobsExpected;
        private long numberOfRecordsExpected;
        private double minRecordsPerSecond;

        private Builder() {
        }

        public Builder numberOfJobsExpected(int numberOfJobsExpected) {
            this.numberOfJobsExpected = numberOfJobsExpected;
            return this;
        }

        public Builder numberOfRecordsExpected(long numberOfRecordsExpected) {
            this.numberOfRecordsExpected = numberOfRecordsExpected;
            return this;
        }

        public Builder minRecordsPerSecond(double minRecordsPerSecond) {
            this.minRecordsPerSecond = minRecordsPerSecond;
            return this;
        }

        public CompactionPerformanceValidator build() {
            return new CompactionPerformanceValidator(this);
        }
    }
}
