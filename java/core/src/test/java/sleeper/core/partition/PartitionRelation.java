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
 * The result of splitting or joining partitions in a factory. Holds builders for the resulting partitions in case they
 * need to be further modified.
 */
public class PartitionRelation {
    private final Partition parent;
    private final List<Partition> children;

    private PartitionRelation(Builder builder) {
        parent = Objects.requireNonNull(builder.parent, "parent must not be null");
        children = Objects.requireNonNull(builder.children, "children must not be null");
        if (children.size() != 2) {
            throw new IllegalArgumentException("Must have 2 children");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public Partition getParent() {
        return parent;
    }

    public List<Partition> getChildren() {
        return children;
    }

    public Partition getLeftChild() {
        return children.get(0);
    }

    public Partition getRightChild() {
        return children.get(1);
    }

    /**
     * A builder to create the partition splitting result.
     */
    public static final class Builder {
        private Partition parent;
        private List<Partition> children;

        public Builder() {
        }

        /**
         * Sets the builder for the parent partition.
         *
         * @param  parent the parent partition builder
         * @return        this builder
         */
        public Builder parent(Partition parent) {
            this.parent = parent;
            return this;
        }

        /**
         * Sets the builders for the child partitions.
         *
         * @param  children the child partition builders
         * @return          this builder
         */
        public Builder children(List<Partition> children) {
            this.children = children;
            return this;
        }

        public PartitionRelation build() {
            return new PartitionRelation(this);
        }
    }
}
