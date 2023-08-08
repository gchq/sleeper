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

package sleeper.core.statestore;

import java.time.Instant;
import java.util.Objects;

public class FileInfoV2 {
    private final String filename;
    private final String partitionId;
    private final FileDataStatistics statistics;
    private final FileInfoV2 parent;
    private final String compactionJobId;
    private final Instant updateTime;

    private FileInfoV2(Builder builder) {
        filename = builder.filename;
        partitionId = builder.partitionId;
        statistics = builder.statistics;
        parent = builder.parent;
        compactionJobId = builder.compactionJobId;
        updateTime = builder.updateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileInfoV2 that = (FileInfoV2) o;
        return Objects.equals(filename, that.filename) && Objects.equals(partitionId, that.partitionId) && Objects.equals(statistics, that.statistics) && Objects.equals(parent, that.parent) && Objects.equals(compactionJobId, that.compactionJobId) && Objects.equals(updateTime, that.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, partitionId, statistics, parent, compactionJobId, updateTime);
    }

    public static final class Builder {
        private String filename;
        private String partitionId;
        private FileDataStatistics statistics;
        private FileInfoV2 parent;
        private String compactionJobId;
        private Instant updateTime;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder partitionId(String partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder statistics(FileDataStatistics statistics) {
            this.statistics = statistics;
            return this;
        }

        public Builder parent(FileInfoV2 parent) {
            this.parent = parent;
            return this;
        }

        public Builder compactionJobId(String compactionJobId) {
            this.compactionJobId = compactionJobId;
            return this;
        }

        public Builder updateTime(Instant updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public FileInfoV2 build() {
            return new FileInfoV2(this);
        }
    }
}
