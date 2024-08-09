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
package sleeper.splitter.find;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.util.List;

import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionSplitCheck {

    private final long numberOfRecordsInPartition;
    private final long splitThreshold;

    private PartitionSplitCheck(long numberOfRecordsInPartition, long splitThreshold) {
        this.numberOfRecordsInPartition = numberOfRecordsInPartition;
        this.splitThreshold = splitThreshold;
    }

    public long getNumberOfRecordsInPartition() {
        return numberOfRecordsInPartition;
    }

    public boolean isNeedsSplitting() {
        return numberOfRecordsInPartition >= splitThreshold;
    }

    public static PartitionSplitCheck fromFilesInPartition(TableProperties properties, List<FileReference> relevantFiles) {
        return fromFilesInPartition(properties.getLong(PARTITION_SPLIT_THRESHOLD), relevantFiles);
    }

    public static PartitionSplitCheck fromFilesInPartition(long splitThreshold, List<FileReference> relevantFiles) {
        long numberOfRecordsInPartition = relevantFiles.stream().map(FileReference::getNumberOfRecords).mapToLong(Long::longValue).sum();
        return new PartitionSplitCheck(numberOfRecordsInPartition, splitThreshold);
    }

}
