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
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Reports on all the references for an individual physical file. A file may be referenced in a number of different
 * partitions. If a file is referenced in multiple partitions, the ranges covered by those partitions must not overlap,
 * or the records in the overlapping portion may be duplicated.
 */
public class AllReferencesToAFile {

    private final String filename;
    private final Instant lastStateStoreUpdateTime;
    private final Map<String, FileReference> referenceByPartitionId;
    private final int referenceCount;

    private AllReferencesToAFile(Builder builder) {
        filename = Objects.requireNonNull(builder.filename, "filename must not be null");
        lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
        referenceByPartitionId = Objects.requireNonNull(builder.referenceByPartitionId, "referenceByPartitionId must not be null");
        referenceCount = referenceByPartitionId.size();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a record for a file referenced in a single partition. This is used in state store implementations to
     * model the file when only a reference was provided to add the file to the state store.
     *
     * @param  reference  the reference to the file
     * @param  updateTime the update time in the state store (should be set by the state store implementation)
     * @return            the file record
     */
    public static AllReferencesToAFile fileWithOneReference(FileReference reference, Instant updateTime) {
        return builder()
                .filename(reference.getFilename())
                .referenceByPartitionId(Map.of(
                        reference.getPartitionId(),
                        reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
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
                        .references(entry.getValue())
                        .build());
    }

    /**
     * Aggregates file references to create a record for each referenced file. This is used in state store
     * implementations to convert to the internal model when only references were provided to add files to the state
     * store. Every reference to each file must be included in the input, or the resulting model will be incorrect.
     *
     * @param  references references to files, including every reference to each file
     * @return            records for the referenced files
     */
    public static List<AllReferencesToAFile> newFilesWithReferences(Collection<FileReference> references) {
        return newFilesWithReferences(references.stream())
                .collect(toUnmodifiableList());
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
                .references(referenceByPartitionId.values().stream()
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

    public int getReferenceCount() {
        return referenceCount;
    }

    public Collection<FileReference> getReferences() {
        return referenceByPartitionId.values();
    }

    public Builder toBuilder() {
        return builder()
                .filename(filename)
                .referenceByPartitionId(referenceByPartitionId)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime);
    }

    @Override
    public String toString() {
        return "AllReferencesToAFile{filename=" + filename +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                ", referenceByPartitionId=" + referenceByPartitionId +
                ", referenceCount=" + referenceCount +
                "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, lastStateStoreUpdateTime, referenceByPartitionId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AllReferencesToAFile)) {
            return false;
        }
        AllReferencesToAFile other = (AllReferencesToAFile) obj;
        return Objects.equals(filename, other.filename)
                && Objects.equals(lastStateStoreUpdateTime, other.lastStateStoreUpdateTime)
                && Objects.equals(referenceByPartitionId, other.referenceByPartitionId);
    }

    /**
     * Builder to create a file record.
     */
    public static final class Builder {
        private String filename;
        private Instant lastStateStoreUpdateTime;
        private Map<String, FileReference> referenceByPartitionId;

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
         * reference to the file as well. This should only be called by an implementation of the state store.
         *
         * @param  lastStateStoreUpdateTime the update time (should be set by the state store implementation)
         * @return                          the builder
         */
        public Builder lastStateStoreUpdateTime(Instant lastStateStoreUpdateTime) {
            this.lastStateStoreUpdateTime = lastStateStoreUpdateTime;
            return this;
        }

        /**
         * Sets the references to the file on partitions.
         *
         * @param  references the references
         * @return            the builder
         */
        public Builder references(Stream<FileReference> references) {
            Map<String, FileReference> map = new TreeMap<>();
            references.forEach(reference -> map.put(reference.getPartitionId(), reference));
            return referenceByPartitionId(Collections.unmodifiableMap(map));
        }

        /**
         * Sets the references to the file on partitions.
         *
         * @param  references the references
         * @return            the builder
         */
        public Builder references(Collection<FileReference> references) {
            return references(references.stream());
        }

        private Builder referenceByPartitionId(Map<String, FileReference> referenceByPartitionId) {
            this.referenceByPartitionId = referenceByPartitionId;
            return this;
        }

        public AllReferencesToAFile build() {
            return new AllReferencesToAFile(this);
        }
    }
}
