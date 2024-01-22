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

package sleeper.core.statestore;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reports on all the references for an individual physical file. A file may be referenced in a number of different
 * partitions, and may also have other external references which contribute to a combined reference count.
 */
public class FileReferences {

    private final String filename;
    private final Instant lastUpdateTime;
    private final int totalReferenceCount;
    private final List<FileReference> internalReferences;

    private FileReferences(Builder builder) {
        filename = builder.filename;
        lastUpdateTime = builder.lastUpdateTime;
        totalReferenceCount = builder.totalReferenceCount;
        internalReferences = builder.internalReferences;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static List<FileReferences> listNewFilesWithReferences(List<FileReference> references, Instant updateTime) {
        return newFilesWithReferences(references, updateTime)
                .collect(Collectors.toUnmodifiableList());
    }

    public static Stream<FileReferences> newFilesWithReferences(List<FileReference> references, Instant updateTime) {
        Map<String, List<FileReference>> referencesByFilename = references.stream()
                .collect(Collectors.groupingBy(FileReference::getFilename, TreeMap::new, Collectors.toList()));
        return referencesByFilename.entrySet().stream()
                .map(entry -> FileReferences.builder()
                        .filename(entry.getKey())
                        .internalReferencesUpdatedAt(entry.getValue(), updateTime)
                        .totalReferenceCount(entry.getValue().size())
                        .lastUpdateTime(updateTime)
                        .build());
    }

    public FileReferences splitReferenceFromPartition(
            String partitionId, Instant updateTime, List<FileReference> newReferences) {
        return toBuilder()
                .internalReferences(Stream.concat(
                                internalReferences.stream()
                                        .filter(reference -> !partitionId.equals(reference.getPartitionId())),
                                newReferences.stream())
                        .collect(Collectors.toUnmodifiableList()))
                .totalReferenceCount(totalReferenceCount - 1 + newReferences.size())
                .lastUpdateTime(updateTime)
                .build();
    }

    public FileReferences withJobIdForPartitions(String jobId, Set<String> partitionUpdates, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferences.stream()
                        .map(reference -> {
                            if (partitionUpdates.contains(reference.getPartitionId())) {
                                return reference.toBuilder().jobId(jobId).lastStateStoreUpdateTime(updateTime).build();
                            } else {
                                return reference;
                            }
                        }).collect(Collectors.toUnmodifiableList()))
                .lastUpdateTime(updateTime)
                .build();
    }

    public String getFilename() {
        return filename;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public int getTotalReferenceCount() {
        return totalReferenceCount;
    }

    public int getExternalReferenceCount() {
        return totalReferenceCount - internalReferences.size();
    }

    public List<FileReference> getInternalReferences() {
        return internalReferences;
    }

    private Builder toBuilder() {
        return builder()
                .filename(filename)
                .internalReferences(internalReferences)
                .totalReferenceCount(totalReferenceCount)
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
        FileReferences that = (FileReferences) o;
        return totalReferenceCount == that.totalReferenceCount && Objects.equals(filename, that.filename) && Objects.equals(lastUpdateTime, that.lastUpdateTime) && Objects.equals(internalReferences, that.internalReferences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, lastUpdateTime, totalReferenceCount, internalReferences);
    }

    @Override
    public String toString() {
        return "FileReferences{" +
                "filename='" + filename + '\'' +
                ", lastUpdateTime=" + lastUpdateTime +
                ", totalReferenceCount=" + totalReferenceCount +
                ", references=" + internalReferences +
                '}';
    }

    public static final class Builder {
        private String filename;
        private Instant lastUpdateTime;
        private int totalReferenceCount;
        private List<FileReference> internalReferences;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder totalReferenceCount(int totalReferenceCount) {
            this.totalReferenceCount = totalReferenceCount;
            return this;
        }

        public Builder internalReferences(List<FileReference> references) {
            this.internalReferences = references;
            return this;
        }

        public Builder internalReferencesUpdatedAt(List<FileReference> internalReferences, Instant updateTime) {
            return internalReferences(internalReferences.stream()
                    .map(fileReference -> fileReference.toBuilder().lastStateStoreUpdateTime(updateTime).build())
                    .collect(Collectors.toUnmodifiableList()));
        }

        public FileReferences build() {
            return new FileReferences(this);
        }
    }
}
