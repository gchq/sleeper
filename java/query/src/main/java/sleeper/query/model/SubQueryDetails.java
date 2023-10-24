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

package sleeper.query.model;

import sleeper.core.range.Region;

import java.util.List;
import java.util.Objects;

public class SubQueryDetails {
    private final String subQueryId;
    private final String leafPartitionId;
    private final Region partitionRegion;
    private final List<String> files;

    private SubQueryDetails(Builder builder) {
        subQueryId = builder.subQueryId;
        leafPartitionId = builder.leafPartitionId;
        partitionRegion = builder.partitionRegion;
        files = builder.files;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSubQueryId() {
        return subQueryId;
    }

    public String getLeafPartitionId() {
        return leafPartitionId;
    }

    public Region getPartitionRegion() {
        return partitionRegion;
    }

    public List<String> getFiles() {
        return files;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        SubQueryDetails that = (SubQueryDetails) object;
        return Objects.equals(subQueryId, that.subQueryId) && Objects.equals(leafPartitionId, that.leafPartitionId) && Objects.equals(partitionRegion, that.partitionRegion) && Objects.equals(files, that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subQueryId, leafPartitionId, partitionRegion, files);
    }

    @Override
    public String toString() {
        return "SubQueryDetails{" +
                "subQueryId='" + subQueryId + '\'' +
                ", leafPartitionId='" + leafPartitionId + '\'' +
                ", partitionRegion=" + partitionRegion +
                ", files=" + files +
                '}';
    }

    public static final class Builder {
        private String subQueryId;
        private String leafPartitionId;
        private Region partitionRegion;
        private List<String> files;

        private Builder() {
        }

        public Builder subQueryId(String subQueryId) {
            this.subQueryId = subQueryId;
            return this;
        }

        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        public Builder partitionRegion(Region partitionRegion) {
            this.partitionRegion = partitionRegion;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public SubQueryDetails build() {
            return new SubQueryDetails(this);
        }
    }
}
