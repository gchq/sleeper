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
    private final List<FileReference> references;

    private AllReferencesToAFile(Builder builder) {
        filename = Objects.requireNonNull(builder.filename, "filename must not be null");
        lastStateStoreUpdateTime = builder.lastStateStoreUpdateTime;
        references = Objects.requireNonNull(builder.references, "references must not be null");
    }

    public static Builder builder() {
        return new Builder();
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
                .references(references.stream()
                        .map(reference -> reference.toBuilder().lastStateStoreUpdateTime(updateTime).build())
                        .toList())
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
        return references.size();
    }

    public List<FileReference> getReferences() {
        return references;
    }

    public Builder toBuilder() {
        return builder()
                .filename(filename)
                .references(references)
                .lastStateStoreUpdateTime(lastStateStoreUpdateTime);
    }

    @Override
    public String toString() {
        return "AllReferencesToAFile{filename=" + filename +
                ", lastStateStoreUpdateTime=" + lastStateStoreUpdateTime +
                ", references=" + references +
                "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, lastStateStoreUpdateTime, references);
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
                && Objects.equals(references, other.references);
    }

    /**
     * Builder to create a file record.
     */
    public static final class Builder {
        private String filename;
        private Instant lastStateStoreUpdateTime;
        private List<FileReference> references;

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
        public Builder references(List<FileReference> references) {
            this.references = references;
            return this;
        }

        public AllReferencesToAFile build() {
            return new AllReferencesToAFile(this);
        }
    }
}
