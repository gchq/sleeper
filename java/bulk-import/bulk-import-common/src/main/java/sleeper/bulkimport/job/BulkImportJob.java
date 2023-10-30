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
package sleeper.bulkimport.job;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import sleeper.ingest.job.IngestJob;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POJO containing information needed to run a bulk import job.
 */
public class BulkImportJob {
    private final String id;
    private final String tableName;
    private final List<String> files;
    private final String className;
    private final Map<String, String> platformSpec;
    private final Map<String, String> sparkConf;

    private BulkImportJob(Builder builder) {
        this.id = builder.id;
        this.tableName = builder.tableName;
        this.files = builder.files;
        this.className = builder.className;
        this.platformSpec = builder.platformSpec;
        this.sparkConf = builder.sparkConf;
    }

    public static BulkImportJob.Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getFiles() {
        return files;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getPlatformSpec() {
        return platformSpec;
    }

    public Map<String, String> getSparkConf() {
        return sparkConf;
    }

    public IngestJob toIngestJob() {
        return IngestJob.builder().files(files).id(id).tableName(tableName).build();
    }

    public BulkImportJob applyIngestJobChanges(IngestJob job) {
        return toBuilder().id(job.getId()).files(job.getFiles()).tableName(job.getTableName()).build();
    }

    public Builder toBuilder() {
        return builder().id(id).files(files).tableName(tableName)
                .className(className).platformSpec(platformSpec).sparkConf(sparkConf);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BulkImportJob that = (BulkImportJob) o;

        return new EqualsBuilder()
                .append(files, that.files)
                .append(id, that.id)
                .append(tableName, that.tableName)
                .append(className, that.className)
                .append(sparkConf, that.sparkConf)
                .append(platformSpec, that.platformSpec)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(files)
                .append(id)
                .append(tableName)
                .append(className)
                .append(sparkConf)
                .append(platformSpec)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("files", files)
                .append("id", id)
                .append("tableName", tableName)
                .append("className", className)
                .append("sparkConf", sparkConf)
                .append("platformSpec", platformSpec)
                .toString();
    }

    public static final class Builder {
        private Map<String, String> sparkConf;
        private String className;
        private String id;
        private List<String> files;
        private String tableName;
        private Map<String, String> platformSpec;

        public Builder() {
        }

        public Builder sparkConf(Map<String, String> sparkConf) {
            this.sparkConf = sparkConf;
            return this;
        }

        public Builder sparkConf(String key, String value) {
            if (this.sparkConf == null) {
                this.sparkConf = new HashMap<>();
            }
            this.sparkConf.put(key, value);

            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder className(String className) {
            this.className = className;
            return this;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public Builder platformSpec(Map<String, String> platformSpec) {
            this.platformSpec = platformSpec;
            return this;
        }

        public BulkImportJob build() {
            return new BulkImportJob(this);
        }
    }
}
