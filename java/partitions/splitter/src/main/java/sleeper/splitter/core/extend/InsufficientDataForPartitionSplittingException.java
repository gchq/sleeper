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
package sleeper.splitter.core.extend;

public class InsufficientDataForPartitionSplittingException extends RuntimeException {

    private final int minLeafPartitions;
    private final int maxLeafPartitionsAfterSplits;

    public InsufficientDataForPartitionSplittingException(int minLeafPartitions, int maxLeafPartitionsAfterSplits) {
        super("Required " + minLeafPartitions + " minimum leaf partitions. Unable to reach more than "
                + maxLeafPartitionsAfterSplits + " leaf partitions based on the given data. Either there are not " +
                "enough unique values for the row key fields, or not enough data was provided.");
        this.minLeafPartitions = minLeafPartitions;
        this.maxLeafPartitionsAfterSplits = maxLeafPartitionsAfterSplits;
    }

    public int getMinLeafPartitions() {
        return minLeafPartitions;
    }

    public int getMaxLeafPartitionsAfterSplits() {
        return maxLeafPartitionsAfterSplits;
    }

}
