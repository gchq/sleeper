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

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;

public class PartitionSplitCheck {

    private final long estimatedRecordsInPartition;
    private final long knownRecordsInPartition;
    private final long splitThreshold;
    private final List<FileReference> filesToComputeSplitPointsFrom;

    private PartitionSplitCheck(long estimatedRecordsInPartition, long numberOfRecordsInPartition, long splitThreshold, List<FileReference> filesToComputeSplitPointsFrom) {
        this.estimatedRecordsInPartition = estimatedRecordsInPartition;
        this.knownRecordsInPartition = numberOfRecordsInPartition;
        this.splitThreshold = splitThreshold;
        this.filesToComputeSplitPointsFrom = filesToComputeSplitPointsFrom;
    }

    public long getEstimatedRecordsInPartition() {
        return estimatedRecordsInPartition;
    }

    public long getKnownRecordsInPartition() {
        return knownRecordsInPartition;
    }

    public boolean isNeedsSplitting() {
        return knownRecordsInPartition >= splitThreshold;
    }

    public List<FileReference> getFilesToComputeSplitPointsFrom() {
        return filesToComputeSplitPointsFrom;
    }

    public static PartitionSplitCheck fromFilesInPartition(TableProperties properties, List<FileReference> partitionFiles) {
        return fromFilesInPartition(properties.getLong(PARTITION_SPLIT_THRESHOLD), partitionFiles);
    }

    public static PartitionSplitCheck fromFilesInPartition(long splitThreshold, List<FileReference> partitionFiles) {
        long estimated = partitionFiles.stream().mapToLong(FileReference::getNumberOfRecords).sum();
        long known = partitionFiles.stream().filter(not(FileReference::isCountApproximate)).mapToLong(FileReference::getNumberOfRecords).sum();
        List<FileReference> filesWhollyInPartition = partitionFiles.stream().filter(FileReference::onlyContainsDataForThisPartition).collect(toUnmodifiableList());
        return new PartitionSplitCheck(estimated, known, splitThreshold, filesWhollyInPartition);
    }

}
