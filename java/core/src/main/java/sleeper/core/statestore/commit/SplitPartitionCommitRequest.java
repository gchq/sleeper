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
package sleeper.core.statestore.commit;

import sleeper.core.partition.Partition;

import java.util.Objects;

/**
 * A request to commit to the state store when we split a partition into new child partitions.
 */
public class SplitPartitionCommitRequest {

    private final Partition parentPartition;
    private final Partition leftChild;
    private final Partition rightChild;

    public SplitPartitionCommitRequest(Partition parentPartition, Partition leftChild, Partition rightChild) {
        this.parentPartition = parentPartition;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentPartition, leftChild, rightChild);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SplitPartitionCommitRequest)) {
            return false;
        }
        SplitPartitionCommitRequest other = (SplitPartitionCommitRequest) obj;
        return Objects.equals(parentPartition, other.parentPartition) && Objects.equals(leftChild, other.leftChild) && Objects.equals(rightChild, other.rightChild);
    }

    @Override
    public String toString() {
        return "SplitPartitionCommitRequest{parentPartition=" + parentPartition + ", leftChild=" + leftChild + ", rightChild=" + rightChild + "}";
    }

}
