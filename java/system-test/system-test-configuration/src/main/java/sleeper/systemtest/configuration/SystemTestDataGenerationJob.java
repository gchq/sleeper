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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.IngestQueue;
import sleeper.core.properties.table.TableProperties;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_INGESTS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_ROWS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;

public class SystemTestDataGenerationJob {

    private final String jobId;
    private final String configBucket;
    private final String roleArnToLoadConfig;
    private final String tableName;
    private final SystemTestIngestMode ingestMode;
    private final IngestQueue ingestQueue;
    private final int numberOfIngests;
    private final long rowsPerIngest;
    private final SystemTestRandomDataSettings randomDataSettings;

    private SystemTestDataGenerationJob(Builder builder) {
        jobId = Optional.ofNullable(builder.jobId).orElseGet(() -> UUID.randomUUID().toString());
        configBucket = builder.configBucket;
        roleArnToLoadConfig = builder.roleArnToLoadConfig;
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        ingestMode = Optional.ofNullable(builder.ingestMode).orElse(SystemTestIngestMode.DIRECT);
        ingestQueue = Optional.ofNullable(builder.ingestQueue).orElse(IngestQueue.STANDARD_INGEST);
        numberOfIngests = builder.numberOfIngests;
        rowsPerIngest = builder.rowsPerIngest;
        randomDataSettings = Optional.ofNullable(builder.randomDataSettings).orElseGet(SystemTestRandomDataSettings::fromDefaults);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<SystemTestDataGenerationJob> getDefaultJobs(
            SystemTestProperties properties, TableProperties tableProperties) {
        return IntStream.range(0, properties.getInt(NUMBER_OF_WRITERS))
                .mapToObj(i -> builder()
                        .instanceProperties(properties)
                        .testProperties(properties.testPropertiesOnly())
                        .tableName(tableProperties.get(TABLE_NAME))
                        .build())
                .toList();
    }

    public static SystemTestDataGenerationJob getDefaultJob(
            SystemTestProperties properties, TableProperties tableProperties) {
        return builder()
                .instanceProperties(properties)
                .testProperties(properties.testPropertiesOnly())
                .tableName(tableProperties.get(TABLE_NAME))
                .build();
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

    public long getRowsPerIngest() {
        return rowsPerIngest;
    }

    public SystemTestRandomDataSettings getRandomDataSettings() {
        return randomDataSettings;
    }

    @Override
    public String toString() {
        return "SystemTestClusterJob{jobId=" + jobId + ", configBucket=" + configBucket + ", roleArnToLoadConfig=" + roleArnToLoadConfig + ", tableName=" + tableName + ", ingestMode=" + ingestMode
                + ", ingestQueue=" + ingestQueue + ", numberOfIngests=" + numberOfIngests + ", rowsPerIngest=" + rowsPerIngest + ", randomDataSettings=" + randomDataSettings + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, configBucket, roleArnToLoadConfig, tableName, ingestMode, ingestQueue, numberOfIngests, rowsPerIngest, randomDataSettings);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SystemTestDataGenerationJob)) {
            return false;
        }
        SystemTestDataGenerationJob other = (SystemTestDataGenerationJob) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(configBucket, other.configBucket) && Objects.equals(roleArnToLoadConfig, other.roleArnToLoadConfig)
                && Objects.equals(tableName, other.tableName) && ingestMode == other.ingestMode && ingestQueue == other.ingestQueue && numberOfIngests == other.numberOfIngests
                && rowsPerIngest == other.rowsPerIngest && Objects.equals(randomDataSettings, other.randomDataSettings);
    }

    public static class Builder {
        private String jobId;
        private String configBucket;
        private String roleArnToLoadConfig;
        private String tableName;
        private SystemTestIngestMode ingestMode;
        private IngestQueue ingestQueue;
        private int numberOfIngests = 1;
        private long rowsPerIngest;
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

        public Builder instanceProperties(InstanceProperties properties) {
            return configBucket(properties.get(CONFIG_BUCKET))
                    .roleArnToLoadConfig(properties.get(INGEST_BY_QUEUE_ROLE_ARN));
        }

        public Builder testProperties(SystemTestPropertyValues properties) {
            return ingestMode(properties.getEnumValue(INGEST_MODE, SystemTestIngestMode.class))
                    .ingestQueue(properties.getEnumValue(INGEST_QUEUE, IngestQueue.class))
                    .numberOfIngests(properties.getInt(NUMBER_OF_INGESTS_PER_WRITER))
                    .rowsPerIngest(properties.getLong(NUMBER_OF_ROWS_PER_INGEST))
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

        public Builder rowsPerIngest(long rowsPerIngest) {
            this.rowsPerIngest = rowsPerIngest;
            return this;
        }

        public Builder randomDataSettings(SystemTestRandomDataSettings randomDataSettings) {
            this.randomDataSettings = randomDataSettings;
            return this;
        }

        public Builder applyMutation(Consumer<Builder> mutator) {
            mutator.accept(this);
            return this;
        }

        public SystemTestDataGenerationJob build() {
            return new SystemTestDataGenerationJob(this);
        }

    }

}
