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

    /**
     * Creates a record for a file referenced in a single partition. This is used in state store implementations to
     * model
     * the file when only a reference was provided to add the file to the state store.
     *
     * @param  reference  the reference to the file
     * @param  updateTime the update time in the state store (should be set by the state store implementation)
     * @return            the file record
     */
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

    /**
     * Aggregates file references to create a record for each referenced file. This is used in state store
     * implementations to convert to the internal model when only references were provided to add files to the state
     * store. Every reference to each file must be included in the input, or the resulting model will be incorrect.
     *
     * @param  references references to files, including every reference to each file
     * @return            a stream of records for the referenced files
     */
    public static Stream<AllReferencesToAFile> newFilesWithReferences(Stream<FileReference> references) {
        Map<String, List<FileReference>> referencesByFilename = references
                .collect(Collectors.groupingBy(FileReference::getFilename, TreeMap::new, Collectors.toUnmodifiableList()));
        return referencesByFilename.entrySet().stream()
                .map(entry -> AllReferencesToAFile.builder()
                        .filename(entry.getKey())
                        .internalReferences(entry.getValue())
                        .totalReferenceCount(entry.getValue().size())
                        .build());
    }

    /**
     * Creates a copy of this model with the reference on one partition removed, and replaced with new references. This
     * is used in state store implementations to split a file reference into two to move the file down the tree of
     * partitions. The new references should cover all the records that were previously covered by the reference that's
     * being removed.
     *
     * @param  partitionId   the ID of the partition to remove the file from
     * @param  newReferences the references to add
     * @param  updateTime    the update time that this occurs (should be set by the state store implementation)
     * @return               a copy of the file record with this change applied
     */
    public AllReferencesToAFile splitReferenceFromPartition(
            String partitionId, Collection<FileReference> newReferences, Instant updateTime) {
        return toBuilder()
                .internalReferences(Stream.concat(
                        internalReferenceByPartitionId.values().stream()
                                .filter(reference -> !partitionId.equals(reference.getPartitionId())),
                        newReferences.stream().map(reference -> reference.toBuilder().lastStateStoreUpdateTime(updateTime).build())))
                .totalReferenceCount(totalReferenceCount - 1 + newReferences.size())
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    /**
     * Creates a copy of this model with the reference on one partition removed. This is used in state store
     * implementations when adding the output of a compaction in a new file that contains all the records for a certain
     * partition. This means that the input files for the compaction must no longer be referenced in that partition.
     *
     * @param  partitionId the ID of the partition to remove the file from
     * @param  updateTime  the update time that this occurs (should be set by the state store implementation)
     * @return             a copy of the file record with this change applied
     */
    public AllReferencesToAFile removeReferenceForPartition(String partitionId, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferenceByPartitionId.values().stream()
                        .filter(reference -> !partitionId.equals(reference.getPartitionId())))
                .totalReferenceCount(totalReferenceCount - 1)
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    /**
     * Creates a copy of this model with the reference on one partition assigned to a job. This is used in state store
     * implementations when assigning a compaction job to its input files. Note that parts of a file are assigned to
     * jobs independently. Each partition that a file is in covers different records. Each reference on each
     * partition will be assigned to and processed by a different job.
     *
     * @param  jobId       the ID of the job to assign the file reference to
     * @param  partitionId the ID of the partition whose reference should be assigned to the job
     * @param  updateTime  the update time that this occurs (should be set by the state store implementation)
     * @return             a copy of the file record with this change applied
     */
    public AllReferencesToAFile withJobIdForPartition(String jobId, String partitionId, Instant updateTime) {
        return toBuilder()
                .internalReferences(internalReferenceByPartitionId.values().stream()
                        .map(reference -> {
                            if (partitionId.equals(reference.getPartitionId())) {
                                return reference.toBuilder().jobId(jobId).lastStateStoreUpdateTime(updateTime).build();
                            } else {
                                return reference;
                            }
                        }))
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    /**
     * Creates a copy of this model with the update time set for the whole file and all its references. This is used in
     * state store implementations when adding a new file to the state store.
     *
     * @param  updateTime the update time that the file is added (should be set by the state store implementation)
     * @return            a copy of the file record with the update time set
     */
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

    /**
     * Retrieves the reference for this file on a given partition.
     *
     * @param  partitionId the ID of the partition to find the reference in
     * @return             the reference to this file in the partition, if the file is referenced in that partition
     */
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
        return totalReferenceCount == that.totalReferenceCount && Objects.equals(filename, that.filename) && Objects.equals(lastStateStoreUpdateTime, that.lastStateStoreUpdateTime)
                && Objects.equals(internalReferenceByPartitionId, that.internalReferenceByPartitionId);
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

    /**
     * Builder to create a file record.
     */
    public static final class Builder {
        private String filename;
        private Instant lastStateStoreUpdateTime;
        private int totalReferenceCount;
        private Map<String, FileReference> internalReferenceByPartitionId;

        private Builder() {
        }

        /**
         * Sets the filename.
         *
         * @param  filename the filename
         * @return          the builder
         */
        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        /**
         * Sets the time of the last state store update. Note that the update time is tracked separately for each
         * internal reference to the file as well. This should only be called by an implementation of the state store.
         *
         * @param  lastStateStoreUpdateTime the update time (should be set by the state store implementation)
         * @return                          the builder
         */
        public Builder lastStateStoreUpdateTime(Instant lastStateStoreUpdateTime) {
            this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
            return this;
        }

        /**
         * Sets the total count of references to the file. This should include internal references in partitions, and
         * any external references, eg. due to running queries reading the file. Note that for any new file this should
         * be equal to the number of internal references.
         *
         * @param  totalReferenceCount the total count of references
         * @return                     the builder
         */
        public Builder totalReferenceCount(int totalReferenceCount) {
            this.totalReferenceCount = totalReferenceCount;
            return this;
        }

        /**
         * Sets the internal references to the file on partitions.
         *
         * @param  references the references
         * @return            the builder
         */
        public Builder internalReferences(Stream<FileReference> references) {
            Map<String, FileReference> map = new TreeMap<>();
            references.forEach(reference -> map.put(reference.getPartitionId(), reference));
            return internalReferenceByPartitionId(Collections.unmodifiableMap(map));
        }

        /**
         * Sets the internal references to the file on partitions.
         *
         * @param  references the references
         * @return            the builder
         */
        public Builder internalReferences(Collection<FileReference> references) {
            return internalReferences(references.stream());
        }

        private Builder internalReferenceByPartitionId(Map<String, FileReference> internalReferenceByPartitionId) {
            this.internalReferenceByPartitionId = internalReferenceByPartitionId;
            return this;
        }

        public AllReferencesToAFile build() {
            return new AllReferencesToAFile(this);
        }
    }
}
