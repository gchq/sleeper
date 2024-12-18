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
package sleeper.ingest.core.job;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.ingest.core.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.core.job.status.IngestJobFailedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A job that will ingest data from one or more files to a Sleeper table. This involves taking the input files, sorting
 * the data in them, writing the data to files split across partitions in the Sleeper table, and adding those new files
 * to the state store.
 */
public class IngestJob {
    private final String id;
    private final String tableName;
    private final String tableId;
    private final List<String> files;

    private IngestJob(Builder builder) {
        id = builder.id;
        tableName = builder.tableName;
        tableId = builder.tableId;
        files = builder.files;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder for an event when files have been added to the state store. Used with the ingest job tracker.
     *
     * @param  writtenTime the time the files were written
     * @return             the builder
     */
    public IngestJobAddedFilesEvent.Builder addedFilesEventBuilder(Instant writtenTime) {
        return IngestJobAddedFilesEvent.builder().jobId(id).tableId(tableId).writtenTime(writtenTime);
    }

    /**
     * Creates a builder for an event when the ingest job failed. Used with the ingest job tracker.
     *
     * @param  runTime the time spent on the failed operation
     * @return         the builder
     */
    public IngestJobFailedEvent.Builder failedEventBuilder(ProcessRunTime runTime) {
        return IngestJobFailedEvent.builder().jobId(id).tableId(tableId).runTime(runTime);
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

    public int getFileCount() {
        return Optional.ofNullable(files).map(List::size).orElse(0);
    }

    public Builder toBuilder() {
        return builder().id(id).files(files).tableName(tableName);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        IngestJob ingestJob = (IngestJob) object;
        return Objects.equals(id, ingestJob.id) && Objects.equals(tableName, ingestJob.tableName)
                && Objects.equals(tableId, ingestJob.tableId) && Objects.equals(files, ingestJob.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tableName, tableId, files);
    }

    @Override
    public String toString() {
        return "IngestJob{" +
                "id='" + id + '\'' +
                ", tableName='" + tableName + '\'' +
                ", tableId='" + tableId + '\'' +
                ", files=" + files +
                '}';
    }

    /**
     * Builder for creating ingest job objects.
     */
    public static final class Builder {
        private String id;
        private String tableName;
        private String tableId;
        private List<String> files;

        private Builder() {
        }

        /**
         * Sets the ingest job ID. This must be unique across all jobs in the Sleeper table.
         *
         * @param  id the ingest job ID
         * @return    the builder
         */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the name of the Sleeper table to write to.
         *
         * @param  tableName the table name
         * @return           the builder
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Sets the internal ID of the Sleeper table to write to. This should only ever be set internally by
         * Sleeper.
         *
         * @param  tableId the internal table ID
         * @return         the builder
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the list of input files.
         *
         * @param  files the list of input files
         * @return       the builder
         */
        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public IngestJob build() {
            return new IngestJob(this);
        }
    }
}
