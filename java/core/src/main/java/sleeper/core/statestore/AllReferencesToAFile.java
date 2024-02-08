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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reports on all the references for an individual physical file. A file may be referenced in a number of different
 * partitions, and may also have other external references which contribute to a combined reference count (eg. a
 * long-running query may count as a reference to the file).
 */
public class AllReferencesToAFile {

    private final String filename;
    private final Instant lastStateStoreUpdateTime;
    private final int totalReferenceCount;
    private final Map<String, FileReference> internalReferenceByPartitionId;

    private AllReferencesToAFile(Builder builder) {
        filename = Objects.requireNonNull(builder.filename, "filename must not be null");
        lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
        totalReferenceCount = builder.totalReferenceCount;
        internalReferenceByPartitionId = Objects.requireNonNull(builder.internalReferenceByPartitionId, "internalReferenceByPartitionId must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static AllReferencesToAFile fileWithOneReference(FileReference reference, Instant updateTime) {
        return builder()
                .filename(reference.getFilename())
                .internalReferenceByPartitionId(Map.of(
                        reference.getPartitionId(),
                        reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
                .totalReferenceCount(1)
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public static Stream<AllReferencesToAFile> newFilesWithReferences(Stream<FileReference> references) {
        return buildersWithReferences(references).map(Builder::build);
    }

    public static Stream<AllReferencesToAFile> newFilesWithReferences(Stream<FileReference> references, Instant updateTime) {
        return buildersWithReferences(
                references.map(reference -> reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
                .map(builder -> builder.lastStateStoreUpdateTime(updateTime).build());
    }

    private static Stream<Builder> buildersWithReferences(Stream<FileReference> references) {
        Map<String, List<FileReference>> referencesByFilename = references
                .collect(Collectors.groupingBy(FileReference::getFilename, TreeMap::new, Collectors.toUnmodifiableList()));
        return referencesByFilename.entrySet().stream()
                .map(entry -> AllReferencesToAFile.builder()
                        .filename(entry.getKey())
                        .internalReferences(entry.getValue())
                        .totalReferenceCount(entry.getValue().size()));
    }

    public AllReferencesToAFile splitReferenceFromPartition(
            String partitionId, Collection<FileReference> newReferences, Instant updateTime) {
        return toBuilder()
                .internalReferences(Stream.concat(
                        internalReferenceByPartitionId.values().stream()
                                .filter(reference -> !partitionId.equals(reference.getPartitionId())),
                        newReferences.stream().map(reference ->
                                reference.toBuilder().lastStateStoreUpdateTime(updateTime).build())))
                .totalReferenceCount(totalReferenceCount - 1 + newReferences.size())
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public AllReferencesToAFile removeReferenceForPartition(String partitionId, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferenceByPartitionId.values().stream()
                        .filter(reference -> !partitionId.equals(reference.getPartitionId())))
                .totalReferenceCount(totalReferenceCount - 1)
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public AllReferencesToAFile addReferences(Collection<FileReference> references, Instant updateTime) {
        return toBuilder()
                .internalReferences(Stream.concat(internalReferenceByPartitionId.values().stream(), references.stream()))
                .totalReferenceCount(totalReferenceCount + references.size())
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public AllReferencesToAFile withJobIdForPartitions(String jobId, Collection<String> partitionUpdates, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferenceByPartitionId.values().stream()
                        .map(reference -> {
                            if (partitionUpdates.contains(reference.getPartitionId())) {
                                return reference.toBuilder().jobId(jobId).lastStateStoreUpdateTime(updateTime).build();
                            } else {
                                return reference;
                            }
                        }))
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public AllReferencesToAFile withCreatedUpdateTime(Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferenceByPartitionId.values().stream()
                        .map(reference -> reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public String getFilename() {
        return filename;
    }

    public Instant getLastStateStoreUpdateTime() {
        return lastStateStoreUpdateTime;
    }

    public int getTotalReferenceCount() {
        return totalReferenceCount;
    }

    public int getExternalReferenceCount() {
        return totalReferenceCount - internalReferenceByPartitionId.size();
    }

    public Collection<FileReference> getInternalReferences() {
        return internalReferenceByPartitionId.values();
    }

    public Optional<FileReference> getReferenceForPartitionId(String partitionId) {
        return Optional.ofNullable(internalReferenceByPartitionId.get(partitionId));
    }

    public Builder toBuilder() {
        return builder()
                .filename(filename)
                .internalReferenceByPartitionId(internalReferenceByPartitionId)
                .totalReferenceCount(totalReferenceCount)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllReferencesToAFile that = (AllReferencesToAFile) o;
        return totalReferenceCount == that.totalReferenceCount && Objects.equals(filename, that.filename) && Objects.equals(lastStateStoreUpdateTime, that.lastStateStoreUpdateTime) && Objects.equals(internalReferenceByPartitionId, that.internalReferenceByPartitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, lastStateStoreUpdateTime, totalReferenceCount, internalReferenceByPartitionId);
    }

    @Override
    public String toString() {
        return "AllReferencesToAFile{" +
                "filename='" + filename + '\'' +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                ", totalReferenceCount=" + totalReferenceCount +
                ", internalReferences=" + internalReferenceByPartitionId.values() +
                '}';
    }

    public static final class Builder {
        private String filename;
        private Instant lastStateStoreUpdateTime;
        private int totalReferenceCount;
        private Map<String, FileReference> internalReferenceByPartitionId;

        private Builder() {
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder lastStateStoreUpdateTime(Instant lastStateStoreUpdateTime) {
            this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
            return this;
        }

        public Builder totalReferenceCount(int totalReferenceCount) {
            this.totalReferenceCount = totalReferenceCount;
            return this;
        }

        public Builder internalReferenceByPartitionId(Map<String, FileReference> internalReferenceByPartitionId) {
            this.internalReferenceByPartitionId = internalReferenceByPartitionId;
            return this;
        }

        public Builder internalReferences(Stream<FileReference> references) {
            Map<String, FileReference> map = new TreeMap<>();
            references.forEach(reference -> map.put(reference.getPartitionId(), reference));
            return internalReferenceByPartitionId(Collections.unmodifiableMap(map));
        }

        public Builder internalReferences(Collection<FileReference> references) {
            return internalReferences(references.stream());
        }

        public AllReferencesToAFile build() {
            return new AllReferencesToAFile(this);
        }
    }
}
