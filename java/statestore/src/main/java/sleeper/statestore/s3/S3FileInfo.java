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

package sleeper.statestore.s3;

import sleeper.core.statestore.FileInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class S3FileInfo {

    private final String filename;
    private final List<FileInfo> internalReferences;
    private final int externalReferences;
    private final Instant lastUpdateTime;

    private S3FileInfo(Builder builder) {
        filename = builder.filename;
        internalReferences = builder.internalReferences;
        externalReferences = builder.externalReferences;
        lastUpdateTime = builder.lastUpdateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<S3FileInfo> newFiles(List<FileInfo> references, Instant updateTime) {
        Map<String, List<FileInfo>> referencesByFilename = references.stream()
                .collect(Collectors.groupingBy(FileInfo::getFilename));
        return referencesByFilename.entrySet().stream()
                .map(entry -> S3FileInfo.builder()
                        .filename(entry.getKey())
                        .internalReferences(entry.getValue().stream()
                                .map(fileInfo -> fileInfo.toBuilder().lastStateStoreUpdateTime(updateTime).build())
                                .collect(Collectors.toUnmodifiableList()))
                        .lastUpdateTime(updateTime)
                        .build())
                .collect(Collectors.toUnmodifiableList());
    }

    public static S3FileInfo active(FileInfo fileInfo, Instant lastUpdateTime) {
        return builder().filename(fileInfo.getFilename()).internalReferences(List.of(fileInfo))
                .lastUpdateTime(lastUpdateTime)
                .build();
    }

    public int getReferenceCount() {
        return internalReferences.size() + externalReferences;
    }

    public String getFilename() {
        return filename;
    }

    public List<FileInfo> getInternalReferences() {
        return internalReferences;
    }

    public int getExternalReferences() {
        return externalReferences;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public S3FileInfo withoutReferenceForPartition(String partitionId) {
        return toBuilder()
                .internalReferences(internalReferences.stream()
                        .filter(reference -> !partitionId.equals(reference.getPartitionId()))
                        .collect(Collectors.toUnmodifiableList()))
                .build();
    }

    public S3FileInfo withJobIdForPartitions(String jobId, Set<String> partitionUpdates, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferences.stream()
                        .map(reference -> {
                            if (partitionUpdates.contains(reference.getPartitionId())) {
                                return reference.toBuilder().jobId(jobId).lastStateStoreUpdateTime(updateTime).build();
                            } else {
                                return reference;
                            }
                        })
                        .collect(Collectors.toUnmodifiableList()))
                .build();
    }

    private Builder toBuilder() {
        return builder()
                .filename(filename)
                .internalReferences(internalReferences)
                .externalReferences(externalReferences)
                .lastUpdateTime(lastUpdateTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3FileInfo that = (S3FileInfo) o;
        return externalReferences == that.externalReferences && Objects.equals(filename, that.filename) && Objects.equals(internalReferences, that.internalReferences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, internalReferences, externalReferences);
    }

    @Override
    public String toString() {
        return "S3FileInfo{" +
                "filename='" + filename + '\'' +
                ", references=" + internalReferences +
                ", externalReferences=" + externalReferences +
                '}';
    }

    public static final class Builder {
        private String filename;
        private List<FileInfo> internalReferences;
        private int externalReferences;
        private Instant lastUpdateTime;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder internalReferences(List<FileInfo> internalReferences) {
            this.internalReferences = internalReferences;
            return this;
        }

        public Builder externalReferences(int externalReferences) {
            this.externalReferences = externalReferences;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public S3FileInfo build() {
            return new S3FileInfo(this);
        }
    }
}
