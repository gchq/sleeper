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

package sleeper.core.partition;

import java.util.List;
import java.util.Objects;

/**
 * The result of splitting or joining partitions in a factory.
 */
public class PartitionRelation {
    private final Partition parent;
    private final Partition leftChild;
    private final Partition rightChild;

    private PartitionRelation(Builder builder) {
        parent = Objects.requireNonNull(builder.parent, "parent must not be null");
        leftChild = Objects.requireNonNull(builder.leftChild, "leftChild must not be null");
        rightChild = Objects.requireNonNull(builder.rightChild, "rightChild must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public Partition getParent() {
        return parent;
    }

    public List<Partition> getChildren() {
        return List.of(leftChild, rightChild);
    }

    public Partition getLeftChild() {
        return leftChild;
    }

    public Partition getRightChild() {
        return rightChild;
    }

    /**
     * A builder to create the partition splitting result.
     */
    public static final class Builder {
        private Partition parent;
        private Partition leftChild;
        private Partition rightChild;

        public Builder() {
        }

        /**
         * Sets the parent partition.
         *
         * @param  parent the parent partition
         * @return        this builder
         */
        public Builder parent(Partition parent) {
            this.parent = parent;
            return this;
        }

        /**
         * Sets the left child partition.
         *
         * @param  leftChild the left child partition
         * @return           this builder
         */
        public Builder leftChild(Partition leftChild) {
            this.leftChild = leftChild;
            return this;
        }

        /**
         * Sets the right child partition.
         *
         * @param  rightChild the right child partition
         * @return            this builder
         */
        public Builder rightChild(Partition rightChild) {
            this.rightChild = rightChild;
            return this;
        }

        public PartitionRelation build() {
            return new PartitionRelation(this);
        }
    }
}
