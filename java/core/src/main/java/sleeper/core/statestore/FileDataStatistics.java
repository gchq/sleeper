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

import sleeper.core.key.Key;

import java.util.Objects;

public class FileDataStatistics {
    private final Key minRowKey;
    private final Key maxRowKey;
    private final Long numberOfRecords;

    private FileDataStatistics(Builder builder) {
        minRowKey = builder.minRowKey;
        maxRowKey = builder.maxRowKey;
        numberOfRecords = builder.numberOfRecords;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Key getMinRowKey() {
        return minRowKey;
    }

    public Key getMaxRowKey() {
        return maxRowKey;
    }

    public Long getNumberOfRecords() {
        return numberOfRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileDataStatistics that = (FileDataStatistics) o;
        return Objects.equals(minRowKey, that.minRowKey) && Objects.equals(maxRowKey, that.maxRowKey) && Objects.equals(numberOfRecords, that.numberOfRecords);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minRowKey, maxRowKey, numberOfRecords);
    }

    public static final class Builder {
        private Key minRowKey;
        private Key maxRowKey;
        private long numberOfRecords;

        private Builder() {
        }

        public Builder minRowKey(Key minRowKey) {
            this.minRowKey = minRowKey;
            return this;
        }

        public Builder maxRowKey(Key maxRowKey) {
            this.maxRowKey = maxRowKey;
            return this;
        }

        public Builder numberOfRecords(long numberOfRecords) {
            this.numberOfRecords = numberOfRecords;
            return this;
        }

        public FileDataStatistics build() {
            return new FileDataStatistics(this);
        }
    }
}
