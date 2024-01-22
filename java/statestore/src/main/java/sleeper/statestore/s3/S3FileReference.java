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

import sleeper.core.statestore.FileReference;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A data structure used by the {@link S3FileReferenceStore} to represent files and their references.
 * Internal references refer to file references that are added by an ingest process or created by compactions.
 * External references are references to this file from outside the {@link S3FileReferenceStore} (e.g. in a long-running query).
 * <p>
 * Note that externalReferenceCount is currently not implemented, and exists as a placeholder.
 */
public class S3FileReference {

    private final String filename;
    private final List<FileReference> internalReferences;
    private final int externalReferenceCount;
    private final Instant lastUpdateTime;

    private S3FileReference(Builder builder) {
        filename = builder.filename;
        internalReferences = builder.internalReferences;
        externalReferenceCount = builder.externalReferenceCount;
        lastUpdateTime = builder.lastUpdateTime;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a list of {@link S3FileReference}s from a list of file references
     *
     * @param references References to files
     * @param updateTime The update time to use when creating a new {@link S3FileReference}
     * @return A list of {@link S3FileReference}s, grouping files by their references.
     */
    public static List<S3FileReference> fromFileReferences(List<FileReference> references, Instant updateTime) {
        return streamFileReferences(references, updateTime).collect(Collectors.toUnmodifiableList());
    }

    /**
     * Creates a stream of {@link S3FileReference}s from a list of file references
     *
     * @param references References to files
     * @param updateTime The update time to use when creating a new {@link S3FileReference}
     * @return A stream of {@link S3FileReference}s, grouping files by their references.
     */
    public static Stream<S3FileReference> streamFileReferences(List<FileReference> references, Instant updateTime) {
        Map<String, List<FileReference>> referencesByFilename = references.stream()
                .collect(Collectors.groupingBy(FileReference::getFilename, TreeMap::new, Collectors.toList()));
        return referencesByFilename.entrySet().stream()
                .map(entry -> S3FileReference.builder()
                        .filename(entry.getKey())
                        .internalReferencesUpdatedAt(entry.getValue(), updateTime)
                        .lastUpdateTime(updateTime)
                        .build());
    }

    public int getReferenceCount() {
        return internalReferences.size() + externalReferenceCount;
    }

    public String getFilename() {
        return filename;
    }

    public List<FileReference> getInternalReferences() {
        return internalReferences;
    }

    public int getExternalReferenceCount() {
        return externalReferenceCount;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public S3FileReference removeReferencesInPartition(String partitionId, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferences.stream()
                        .filter(reference -> !partitionId.equals(reference.getPartitionId()))
                        .collect(Collectors.toUnmodifiableList()))
                .lastUpdateTime(updateTime)
                .build();
    }

    public S3FileReference withJobIdForPartitions(String jobId, Set<String> partitionUpdates, Instant updateTime) {
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

    public S3FileReference withUpdatedReferences(S3FileReference newFile) {
        return toBuilder()
                .internalReferences(Stream.concat(internalReferences.stream(), newFile.internalReferences.stream())
                        .collect(Collectors.toUnmodifiableList()))
                .lastUpdateTime(newFile.lastUpdateTime)
                .build();
    }

    private Builder toBuilder() {
        return builder()
                .filename(filename)
                .internalReferences(internalReferences)
                .externalReferenceCount(externalReferenceCount)
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
        S3FileReference that = (S3FileReference) o;
        return externalReferenceCount == that.externalReferenceCount && Objects.equals(filename, that.filename) && Objects.equals(internalReferences, that.internalReferences) && Objects.equals(lastUpdateTime, that.lastUpdateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, internalReferences, externalReferenceCount, lastUpdateTime);
    }

    @Override
    public String toString() {
        return "S3FileReference{" +
                "filename='" + filename + '\'' +
                ", internalReferences=" + internalReferences +
                ", externalReferenceCount=" + externalReferenceCount +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }

    public static final class Builder {
        private String filename;
        private List<FileReference> internalReferences;
        private int externalReferenceCount;
        private Instant lastUpdateTime;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder internalReferences(List<FileReference> internalReferences) {
            this.internalReferences = internalReferences;
            return this;
        }

        public Builder internalReferencesUpdatedAt(List<FileReference> internalReferences, Instant updateTime) {
            return internalReferences(internalReferences.stream()
                    .map(fileReference -> fileReference.toBuilder().lastStateStoreUpdateTime(updateTime).build())
                    .collect(Collectors.toUnmodifiableList()));
        }

        public Builder externalReferenceCount(int externalReferenceCount) {
            this.externalReferenceCount = externalReferenceCount;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public S3FileReference build() {
            return new S3FileReference(this);
        }
    }
}
