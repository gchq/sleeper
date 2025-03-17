/*
 * Copyright 2022-2025 Crown Copyright
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

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * A request to split a file reference in the state store. The old reference will be deleted and new references will be
 * created in one transaction.
 */
public class SplitFileReferenceRequest {
    private final FileReference oldReference;
    private final List<FileReference> newReferences;

    public SplitFileReferenceRequest(FileReference oldReference, List<FileReference> newReferences) {
        this.oldReference = Objects.requireNonNull(oldReference, "oldReference must not be null");
        this.newReferences = Objects.requireNonNull(newReferences, "newReferences must not be null");
        if (newReferences.isEmpty()) {
            throw new IllegalArgumentException("newReferences must not be empty");
        }
    }

    /**
     * Builds a request to split a file reference into the immediate child partitions of the original partition. This
     * will estimate the number of records in the child partitions by assuming an even split between the two.
     *
     * @param  file           the original file reference
     * @param  leftPartition  the ID of the left child partition
     * @param  rightPartition the ID of the right child partition
     * @return                the request
     */
    public static SplitFileReferenceRequest splitFileToChildPartitions(FileReference file, String leftPartition, String rightPartition) {
        return new SplitFileReferenceRequest(file,
                List.of(SplitFileReference.referenceForChildPartition(file, leftPartition),
                        SplitFileReference.referenceForChildPartition(file, rightPartition)));
    }

    /**
     * Creates a copy of this request with the update times removed in the file references. Used when storing a
     * transaction to apply this request where the update time is held separately.
     *
     * @return the copy
     */
    public SplitFileReferenceRequest withNoUpdateTimes() {
        return new SplitFileReferenceRequest(
                oldReference.toBuilder().lastStateStoreUpdateTime(null).build(),
                newReferences.stream()
                        .map(ref -> ref.toBuilder().lastStateStoreUpdateTime(null).build())
                        .collect(toUnmodifiableList()));
    }

    public String getFilename() {
        return oldReference.getFilename();
    }

    public String getFromPartitionId() {
        return oldReference.getPartitionId();
    }

    public FileReference getOldReference() {
        return oldReference;
    }

    public List<FileReference> getNewReferences() {
        return newReferences;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitFileReferenceRequest that = (SplitFileReferenceRequest) o;
        return Objects.equals(oldReference, that.oldReference) && Objects.equals(newReferences, that.newReferences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldReference, newReferences);
    }

    @Override
    public String toString() {
        return "SplitFileReferenceRequest{" +
                "oldReference=" + oldReference +
                ", newReferences=" + newReferences +
                '}';
    }
}
