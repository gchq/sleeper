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

import java.util.Objects;

public class FileReferenceCount {
    private final Long lastUpdateTime;
    private final String tableId;
    private final String filename;
    private final long numberOfReferences;

    private FileReferenceCount(Builder builder) {
        lastUpdateTime = builder.lastUpdateTime;
        tableId = Objects.requireNonNull(builder.tableId, "tableId must not be null");
        filename = Objects.requireNonNull(builder.filename, "filename must not be null");
        numberOfReferences = builder.numberOfReferences;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder newFile(FileInfo fileReference) {
        return builder()
                .filename(fileReference.getFilename())
                .numberOfReferences(1L);
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getTableId() {
        return tableId;
    }

    public String getFilename() {
        return filename;
    }

    public long getNumberOfReferences() {
        return numberOfReferences;
    }

    public Builder toBuilder() {
        return builder()
                .lastUpdateTime(lastUpdateTime)
                .tableId(tableId)
                .filename(filename)
                .numberOfReferences(numberOfReferences);
    }

    public FileReferenceCount decrement() {
        if (numberOfReferences < 1) {
            throw new IllegalStateException("File has no references");
        }
        return toBuilder()
                .numberOfReferences(numberOfReferences - 1)
                .build();
    }

    public FileReferenceCount increment() {
        return toBuilder()
                .numberOfReferences(numberOfReferences + 1)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileReferenceCount that = (FileReferenceCount) o;
        return lastUpdateTime.equals(that.lastUpdateTime)
                && numberOfReferences == that.numberOfReferences
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastUpdateTime, tableId, filename, numberOfReferences);
    }

    @Override
    public String toString() {
        return "FileReferenceCount{" +
                "lastUpdateTime=" + lastUpdateTime +
                ", tableId='" + tableId + '\'' +
                ", filename='" + filename + '\'' +
                ", numberOfReferences=" + numberOfReferences +
                '}';
    }

    public static final class Builder {
        private long lastUpdateTime;
        private String tableId;
        private String filename;
        private long numberOfReferences;

        private Builder() {
        }

        public Builder lastUpdateTime(long val) {
            lastUpdateTime = val;
            return this;
        }

        public Builder tableId(String val) {
            tableId = val;
            return this;
        }

        public Builder filename(String val) {
            filename = val;
            return this;
        }

        public Builder numberOfReferences(long val) {
            numberOfReferences = val;
            return this;
        }

        public FileReferenceCount build() {
            return new FileReferenceCount(this);
        }
    }
}
