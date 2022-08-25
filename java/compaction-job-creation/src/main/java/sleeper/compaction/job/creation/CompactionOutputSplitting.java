/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.job.creation;

import org.apache.commons.lang3.tuple.MutablePair;
import sleeper.compaction.job.CompactionJob;

import java.util.Arrays;
import java.util.Objects;

public class CompactionOutputSplitting implements CompactionOutput {

    private final String leftPartitionId;
    private final String rightPartitionId;
    private final String leftOutputFile;
    private final String rightOutputFile;
    private final Object splitPoint;
    private final int dimension;

    private CompactionOutputSplitting(Builder builder) {
        leftPartitionId = Objects.requireNonNull(builder.leftPartitionId, "leftPartitionId must not be null");
        rightPartitionId = Objects.requireNonNull(builder.rightPartitionId, "rightPartitionId must not be null");
        leftOutputFile = Objects.requireNonNull(builder.leftOutputFile, "leftOutputFile must not be null");
        rightOutputFile = Objects.requireNonNull(builder.rightOutputFile, "rightOutputFile must not be null");
        splitPoint = Objects.requireNonNull(builder.splitPoint, "splitPoint must not be null");
        dimension = builder.dimension;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void set(CompactionJob job) {
        job.setIsSplittingJob(true);
        job.setChildPartitions(Arrays.asList(leftPartitionId, rightPartitionId));
        job.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        job.setSplitPoint(splitPoint);
        job.setDimension(dimension);
    }

    public static final class Builder {
        private String leftPartitionId;
        private String rightPartitionId;
        private String leftOutputFile;
        private String rightOutputFile;
        private Object splitPoint;
        private int dimension;

        private Builder() {
        }

        public Builder leftPartitionId(String leftPartitionId) {
            this.leftPartitionId = leftPartitionId;
            return this;
        }

        public Builder rightPartitionId(String rightPartitionId) {
            this.rightPartitionId = rightPartitionId;
            return this;
        }

        public Builder leftOutputFile(String leftOutputFile) {
            this.leftOutputFile = leftOutputFile;
            return this;
        }

        public Builder rightOutputFile(String rightOutputFile) {
            this.rightOutputFile = rightOutputFile;
            return this;
        }

        public Builder splitPoint(Object splitPoint) {
            this.splitPoint = splitPoint;
            return this;
        }

        public Builder dimension(int dimension) {
            this.dimension = dimension;
            return this;
        }

        public CompactionOutputSplitting build() {
            return new CompactionOutputSplitting(this);
        }
    }
}
