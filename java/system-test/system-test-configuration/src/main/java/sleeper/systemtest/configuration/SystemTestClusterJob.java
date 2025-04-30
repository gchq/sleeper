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

public class SystemTestClusterJob {

    private final String configBucket;
    private final String roleArnToLoadConfig;
    private final SystemTestIngestMode ingestMode;
    private final IngestQueue ingestQueue;
    private final int numberOfIngests;
    private final int recordsPerIngest;
    private final SystemTestRandomDataSettings randomDataSettings;

    private SystemTestClusterJob(Builder builder) {
        configBucket = builder.configBucket;
        roleArnToLoadConfig = builder.roleArnToLoadConfig;
        ingestMode = builder.ingestMode;
        ingestQueue = builder.ingestQueue;
        numberOfIngests = builder.numberOfIngests;
        recordsPerIngest = builder.recordsPerIngest;
        randomDataSettings = builder.randomDataSettings;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getConfigBucket() {
        return configBucket;
    }

    public String getRoleArnToLoadConfig() {
        return roleArnToLoadConfig;
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

    public static class Builder {
        private String configBucket;
        private String roleArnToLoadConfig;
        private SystemTestIngestMode ingestMode;
        private IngestQueue ingestQueue;
        private int numberOfIngests;
        private int recordsPerIngest;
        private SystemTestRandomDataSettings randomDataSettings;

        private Builder() {
        }

        public Builder configBucket(String configBucket) {
            this.configBucket = configBucket;
            return this;
        }

        public Builder roleArnToLoadConfig(String roleArnToLoadConfig) {
            this.roleArnToLoadConfig = roleArnToLoadConfig;
            return this;
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
