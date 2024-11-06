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
package sleeper.bulkimport.core.job;

import sleeper.ingest.job.IngestJob;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * POJO containing information needed to run a bulk import job.
 */
public class BulkImportJob {
    private final String id;
    private final String tableName;
    private final String tableId;
    private final List<String> files;
    private final String className;
    private final Map<String, String> platformSpec;
    private final Map<String, String> sparkConf;

    private BulkImportJob(Builder builder) {
        id = builder.id;
        tableName = builder.tableName;
        tableId = builder.tableId;
        files = builder.files;
        className = builder.className;
        platformSpec = builder.platformSpec;
        sparkConf = builder.sparkConf;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableId() {
        return tableId;
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
        return IngestJob.builder().files(files).id(id).tableName(tableName).tableId(tableId).build();
    }

    public BulkImportJob applyIngestJobChanges(IngestJob job) {
        return toBuilder()
                .id(job.getId())
                .tableName(job.getTableName())
                .tableId(job.getTableId())
                .files(job.getFiles())
                .build();
    }

    public Builder toBuilder() {
        return builder().id(id).files(files).tableName(tableName).tableId(tableId)
                .className(className).platformSpec(platformSpec).sparkConf(sparkConf);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        BulkImportJob that = (BulkImportJob) object;
        return Objects.equals(id, that.id) && Objects.equals(tableName, that.tableName)
                && Objects.equals(tableId, that.tableId) && Objects.equals(files, that.files)
                && Objects.equals(className, that.className) && Objects.equals(platformSpec, that.platformSpec)
                && Objects.equals(sparkConf, that.sparkConf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tableName, tableId, files, className, platformSpec, sparkConf);
    }

    @Override
    public String toString() {
        return "BulkImportJob{" +
                "id='" + id + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableId='" + tableId + '\'' +
                ", files=" + files +
                ", className='" + className + '\'' +
                ", platformSpec=" + platformSpec +
                ", sparkConf=" + sparkConf +
                '}';
    }

    public static final class Builder {
        private String id;
        private String tableName;
        private String tableId;
        private List<String> files;
        private String className;
        private Map<String, String> platformSpec;
        private Map<String, String> sparkConf;

        public Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public Builder className(String className) {
            this.className = className;
            return this;
        }

        public Builder platformSpec(Map<String, String> platformSpec) {
            this.platformSpec = platformSpec;
            return this;
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

        public BulkImportJob build() {
            return new BulkImportJob(this);
        }
    }
}
