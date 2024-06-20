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
package sleeper.ingest.job.status;

import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status update for when an ingest job has committed files to the state store.
 */
public class IngestJobAddedFilesStatus implements ProcessStatusUpdate {

    private final Instant writtenTime;
    private final Instant updateTime;
    private final int fileCount;

    private IngestJobAddedFilesStatus(Builder builder) {
        this.writtenTime = builder.writtenTime;
        this.updateTime = builder.updateTime;
        this.fileCount = builder.fileCount;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Instant getWrittenTime() {
        return writtenTime;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    public int getFileCount() {
        return fileCount;
    }

    @Override
    public boolean isPartOfRun() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(writtenTime, updateTime, fileCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobAddedFilesStatus)) {
            return false;
        }
        IngestJobAddedFilesStatus other = (IngestJobAddedFilesStatus) obj;
        return Objects.equals(writtenTime, other.writtenTime) && Objects.equals(updateTime, other.updateTime) && fileCount == other.fileCount;
    }

    @Override
    public String toString() {
        return "IngestJobAddedFilesStatus{writtenTime=" + writtenTime + ", updateTime=" + updateTime + ", fileCount=" + fileCount + "}";
    }

    /**
     * Builder to create the status update.
     */
    public static class Builder {
        private Instant writtenTime;
        private Instant updateTime;
        private int fileCount;

        /**
         * Sets the time the file was written.
         *
         * @param  writtenTime the time
         * @return             the builder for chaining
         */
        public Builder writtenTime(Instant writtenTime) {
            this.writtenTime = writtenTime;
            return this;
        }

        /**
         * Sets the time the status update was saved.
         *
         * @param  updateTime the time
         * @return            the builder for chaining
         */
        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        /**
         * Sets the number of files that were committed to the state store for this update.
         *
         * @param  fileCount the number of files
         * @return           the builder for chaining
         */
        public Builder fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        public IngestJobAddedFilesStatus build() {
            return new IngestJobAddedFilesStatus(this);
        }
    }
}
