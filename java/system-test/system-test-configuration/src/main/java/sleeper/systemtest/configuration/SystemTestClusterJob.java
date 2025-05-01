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
package sleeper.systemtest.configuration;

import sleeper.core.properties.validation.IngestQueue;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;

public class SystemTestClusterJob {

    private final String jobId;
    private final String configBucket;
    private final String roleArnToLoadConfig;
    private final String tableName;
    private final SystemTestIngestMode ingestMode;
    private final IngestQueue ingestQueue;
    private final int numberOfIngests;
    private final int recordsPerIngest;
    private final SystemTestRandomDataSettings randomDataSettings;

    private SystemTestClusterJob(Builder builder) {
        jobId = Optional.ofNullable(builder.jobId).orElseGet(() -> UUID.randomUUID().toString());
        configBucket = builder.configBucket;
        roleArnToLoadConfig = builder.roleArnToLoadConfig;
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        ingestMode = Optional.ofNullable(builder.ingestMode).orElse(SystemTestIngestMode.DIRECT);
        ingestQueue = Optional.ofNullable(builder.ingestQueue).orElse(IngestQueue.STANDARD_INGEST);
        numberOfIngests = builder.numberOfIngests;
        recordsPerIngest = builder.recordsPerIngest;
        randomDataSettings = Optional.ofNullable(builder.randomDataSettings).orElseGet(SystemTestRandomDataSettings::fromDefaults);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public String getConfigBucket() {
        return configBucket;
    }

    public String getRoleArnToLoadConfig() {
        return roleArnToLoadConfig;
    }

    public String getTableName() {
        return tableName;
    }

    public SystemTestIngestMode getIngestMode() {
        return ingestMode;
    }

    public IngestQueue getIngestQueue() {
        return ingestQueue;
    }

    public int getNumberOfIngests() {
        return numberOfIngests;
    }

    public int getRecordsPerIngest() {
        return recordsPerIngest;
    }

    public SystemTestRandomDataSettings getRandomDataSettings() {
        return randomDataSettings;
    }

    @Override
    public String toString() {
        return "SystemTestClusterJob{jobId=" + jobId + ", configBucket=" + configBucket + ", roleArnToLoadConfig=" + roleArnToLoadConfig + ", tableName=" + tableName + ", ingestMode=" + ingestMode
                + ", ingestQueue=" + ingestQueue + ", numberOfIngests=" + numberOfIngests + ", recordsPerIngest=" + recordsPerIngest + ", randomDataSettings=" + randomDataSettings + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, configBucket, roleArnToLoadConfig, tableName, ingestMode, ingestQueue, numberOfIngests, recordsPerIngest, randomDataSettings);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SystemTestClusterJob)) {
            return false;
        }
        SystemTestClusterJob other = (SystemTestClusterJob) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(configBucket, other.configBucket) && Objects.equals(roleArnToLoadConfig, other.roleArnToLoadConfig)
                && Objects.equals(tableName, other.tableName) && ingestMode == other.ingestMode && ingestQueue == other.ingestQueue && numberOfIngests == other.numberOfIngests
                && recordsPerIngest == other.recordsPerIngest && Objects.equals(randomDataSettings, other.randomDataSettings);
    }

    public static class Builder {
        private String jobId;
        private String configBucket;
        private String roleArnToLoadConfig;
        private String tableName;
        private SystemTestIngestMode ingestMode;
        private IngestQueue ingestQueue;
        private int numberOfIngests;
        private int recordsPerIngest;
        private SystemTestRandomDataSettings randomDataSettings;

        private Builder() {
        }

        public Builder jobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder configBucket(String configBucket) {
            this.configBucket = configBucket;
            return this;
        }

        public Builder roleArnToLoadConfig(String roleArnToLoadConfig) {
            this.roleArnToLoadConfig = roleArnToLoadConfig;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder properties(SystemTestPropertyValues properties) {
            return ingestMode(properties.getEnumValue(INGEST_MODE, SystemTestIngestMode.class))
                    .ingestQueue(properties.getEnumValue(INGEST_QUEUE, IngestQueue.class))
                    .numberOfIngests(properties.getInt(NUMBER_OF_INGESTS_PER_WRITER))
                    .recordsPerIngest(properties.getInt(NUMBER_OF_RECORDS_PER_INGEST))
                    .randomDataSettings(SystemTestRandomDataSettings.fromProperties(properties));
        }

        public Builder ingestMode(SystemTestIngestMode ingestMode) {
            this.ingestMode = ingestMode;
            return this;
        }

        public Builder ingestQueue(IngestQueue ingestQueue) {
            this.ingestQueue = ingestQueue;
            return this;
        }

        public Builder numberOfIngests(int numberOfIngests) {
            this.numberOfIngests = numberOfIngests;
            return this;
        }

        public Builder recordsPerIngest(int recordsPerIngest) {
            this.recordsPerIngest = recordsPerIngest;
            return this;
        }

        public Builder randomDataSettings(SystemTestRandomDataSettings randomDataSettings) {
            this.randomDataSettings = randomDataSettings;
            return this;
        }

        public SystemTestClusterJob build() {
            return new SystemTestClusterJob(this);
        }

    }

}
