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
package sleeper.systemtest.dsl.compaction;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.systemtest.dsl.sourcedata.IngestSourceFilesContext;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

public class FoundCompactionJobs {

    private final UnaryOperator<String> getFilePath;
    private final List<CompactionJob> jobs;

    private FoundCompactionJobs(UnaryOperator<String> getFilePath, List<CompactionJob> jobs) {
        this.jobs = jobs;
        this.getFilePath = getFilePath;
    }

    public static FoundCompactionJobs from(IngestSourceFilesContext sourceFiles, List<CompactionJob> jobs) {
        return new FoundCompactionJobs(sourceFiles::getFilePath, jobs);
    }

    public FullCompactionCheckSummary checkFullCompactionWithPartitionsAndInputFiles(PartitionTree partitions, String... inputFiles) {
        Set<String> compactedPartitionIds = jobs.stream().map(CompactionJob::getPartitionId).collect(toUnmodifiableSet());
        Set<String> leafPartitionIds = partitions.getLeafPartitions().stream().map(Partition::getId).collect(toUnmodifiableSet());
        Set<String> uncompactedPartitionIds = new HashSet<>(leafPartitionIds);
        uncompactedPartitionIds.removeAll(compactedPartitionIds);
        Set<String> unexpectedPartitionIds = new HashSet<>(compactedPartitionIds);
        unexpectedPartitionIds.removeAll(leafPartitionIds);
        Set<String> foundInputFiles = jobs.stream().map(CompactionJob::getInputFiles).flatMap(List::stream).collect(toUnmodifiableSet());
        Set<String> expectedInputFiles = Stream.of(inputFiles).map(getFilePath).collect(toUnmodifiableSet());
        return new FullCompactionCheckSummary(jobs.size(), uncompactedPartitionIds.size(), unexpectedPartitionIds.size(), foundInputFiles.equals(expectedInputFiles));
    }

    public static FullCompactionCheckSummary expectedFullCompactionCheckWithJobs(int numJobs) {
        return new FullCompactionCheckSummary(numJobs, 0, 0, true);
    }

    public record FullCompactionCheckSummary(int numJobs, int numUncompactedPartitions, int numUnexpectedPartitions, boolean inputFilesMatched) {
    }
}
