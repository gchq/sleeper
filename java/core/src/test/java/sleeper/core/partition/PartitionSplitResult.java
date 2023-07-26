/*
 * Copyright 2022-2023 Crown Copyright
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
import java.util.stream.Collectors;

public class PartitionSplitResult {
    private final Partition.Builder parent;
    private final List<Partition.Builder> children;

    private PartitionSplitResult(Builder builder) {
        parent = builder.parent;
        children = builder.children;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Partition.Builder getParent() {
        return parent;
    }

    public Partition buildParent() {
        return parent.build();
    }

    public List<Partition.Builder> getChildren() {
        return children;
    }

    public List<Partition> buildChildren() {
        return children.stream().map(Partition.Builder::build).collect(Collectors.toList());
    }

    public static final class Builder {
        private Partition.Builder parent;
        private List<Partition.Builder> children;

        public Builder() {
        }

        public Builder parent(Partition.Builder parent) {
            this.parent = parent;
            return this;
        }

        public Builder children(List<Partition.Builder> children) {
            this.children = children;
            return this;
        }

        public PartitionSplitResult build() {
            return new PartitionSplitResult(this);
        }
    }
}
