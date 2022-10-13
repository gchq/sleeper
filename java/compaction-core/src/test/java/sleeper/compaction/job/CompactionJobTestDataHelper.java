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
package sleeper.compaction.job;

import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobRun;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.compaction.job.CompactionJobTestUtils.KEY_FIELD;
import static sleeper.compaction.job.CompactionJobTestUtils.createInstanceProperties;
import static sleeper.compaction.job.CompactionJobTestUtils.createSchema;
import static sleeper.compaction.job.CompactionJobTestUtils.createTableProperties;

public class CompactionJobTestDataHelper {

    public static final String DEFAULT_TASK_ID = "task-id";

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    private List<Partition> partitions;
    private PartitionTree partitionTree;
    private FileInfoFactory fileFactory;

    public Partition singlePartition() {
        return singlePartitionTree().getRootPartition();
    }

    public void partitionTree(Consumer<PartitionsBuilder> config) {
        if (isPartitionsSpecified()) {
            throw new IllegalStateException("Partition tree already initialised");
        }
        setPartitions(createPartitions(config));
    }

    public CompactionJob singleFileCompaction() {
        return singleFileCompaction(singlePartition());
    }

    public CompactionJob singleFileCompaction(String partitionId) {
        return singleFileCompaction(partitionTree.getPartition(partitionId));
    }

    public CompactionJob singleFileCompaction(Partition partition) {
        validatePartitionSpecified(partition);
        return jobFactory.createCompactionJob(
                Collections.singletonList(fileInPartition(partition)),
                partition.getId());
    }

    public CompactionJob singleFileSplittingCompaction(String parentPartitionId, String leftPartitionId, String rightPartitionId) {
        Object splitPoint;
        if (!isPartitionsSpecified()) {
            splitPoint = "p";
            setPartitions(createPartitions(builder -> builder
                    .leavesWithSplits(Arrays.asList(leftPartitionId, rightPartitionId), Collections.singletonList(splitPoint))
                    .parentJoining(parentPartitionId, leftPartitionId, rightPartitionId)));
        } else {
            Partition left = partitionTree.getPartition(leftPartitionId);
            Partition right = partitionTree.getPartition(rightPartitionId);
            splitPoint = getValidSplitPoint(parentPartitionId, left, right);
        }
        return jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileInPartition(partitionTree.getPartition(parentPartitionId))),
                parentPartitionId, leftPartitionId, rightPartitionId, splitPoint, 0);
    }

    public CompactionJobStatus createdCompactionStatus(Instant createTime) {
        return createdCompactionStatus(singleFileCompaction(), createTime);
    }

    public CompactionJobStatus startedCompactionStatus(Instant createTime) {
        return startedCompactionStatus(singleFileCompaction(), createTime);
    }

    public CompactionJobStatus finishedCompactionStatus(
            Instant createTime, Duration runDuration, long linesRead, long linesWritten) {
        return finishedCompactionStatus(singleFileCompaction(), createTime, runDuration, linesRead, linesWritten);
    }

    public CompactionJobStatus compactionStatusWithJobRunsStartToFinish(
            Instant createTime, Consumer<CompactionJobRunsBuilder> runs) {
        return compactionStatusWithJobRunsStartToFinish(singleFileCompaction(), createTime, runs);
    }

    public static CompactionJobStatus createdCompactionStatus(CompactionJob job, Instant createTime) {
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createTime))
                .jobRunsLatestFirst(Collections.emptyList())
                .build();
    }

    public static CompactionJobStatus startedCompactionStatus(CompactionJob job, Instant createTime) {
        Instant startTime = createTime.plus(Duration.ofSeconds(10));
        Instant startUpdateTime = startTime.plus(Duration.ofMillis(123));
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createTime))
                .singleJobRun(CompactionJobRun.started(DEFAULT_TASK_ID,
                        CompactionJobStartedStatus.updateAndStartTime(startUpdateTime, startTime)))
                .build();
    }

    public static CompactionJobStatus finishedCompactionStatus(
            CompactionJob job, Instant createTime, Duration runDuration, long linesRead, long linesWritten) {
        return compactionStatusWithJobRunsStartToFinish(job, createTime, runs -> runs
                .finishedRun(runDuration, linesRead, linesWritten));
    }

    public static CompactionJobStatus compactionStatusWithJobRunsStartToFinish(
            CompactionJob job, Instant createTime, Consumer<CompactionJobRunsBuilder> runs) {
        CompactionJobRunsBuilder runsBuilder = new CompactionJobRunsBuilder(createTime);
        runs.accept(runsBuilder);
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, createTime))
                .jobRunsLatestFirst(runsBuilder.latestFirst())
                .build();
    }

    public static CompactionJobRun finishedJobRun(
            Instant startTime, Duration runDuration, long linesRead, long linesWritten) {
        Instant startUpdateTime = startTime.plus(Duration.ofMillis(123));
        Instant finishTime = startTime.plus(runDuration);
        Instant finishUpdateTime = finishTime.plus(Duration.ofMillis(123));
        CompactionJobSummary summary = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(linesRead, linesWritten), startTime, finishTime);
        return CompactionJobRun.finished(DEFAULT_TASK_ID,
                CompactionJobStartedStatus.updateAndStartTime(startUpdateTime, startTime),
                CompactionJobFinishedStatus.updateTimeAndSummary(finishUpdateTime, summary));
    }

    public static class CompactionJobRunsBuilder {

        private final List<CompactionJobRun> runs = new ArrayList<>();
        private Instant nextStartTime;

        private CompactionJobRunsBuilder(Instant jobCreateTime) {
            this.nextStartTime = jobCreateTime.plus(Duration.ofSeconds(10));
        }

        public CompactionJobRunsBuilder finishedRun(Duration runDuration, long linesRead, long linesWritten) {
            CompactionJobRun run = finishedJobRun(nextStartTime, runDuration, linesRead, linesWritten);
            nextStartTime = run.getFinishTime().plus(Duration.ofSeconds(10));
            runs.add(run);
            return this;
        }

        private List<CompactionJobRun> latestFirst() {
            Collections.reverse(runs);
            return runs;
        }
    }

    private FileInfo fileInPartition(Partition partition) {
        Range range = singleFieldRange(partition);
        String min = range.getMin() + "a";
        String max = range.getMin() + "b";
        return fileFactory.partitionFile(partition, 100L, min, max);
    }

    private PartitionTree singlePartitionTree() {
        if (!isPartitionsSpecified()) {
            setPartitions(createSinglePartition());
        } else if (!partitionTree.getRootPartition().isLeafPartition()) {
            throw new IllegalStateException(
                    "Partition tree already initialised with multiple partitions when single partition expected");
        }
        return partitionTree;
    }

    private void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
        partitionTree = new PartitionTree(schema, partitions);
        fileFactory = FileInfoFactory.builder().schema(schema).partitionTree(partitionTree).build();
    }

    private boolean isPartitionsSpecified() {
        return partitionTree != null;
    }

    private List<Partition> createSinglePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct();
    }

    private List<Partition> createPartitions(Consumer<PartitionsBuilder> config) {
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        config.accept(builder);
        return builder.buildList();
    }

    private void validatePartitionSpecified(Partition checkPartition) {
        for (Partition partition : partitions) {
            if (partition == checkPartition) {
                return;
            }
        }
        throw new IllegalArgumentException("Partition should be specified with helper: " + checkPartition);
    }

    private Object getValidSplitPoint(String parentId, Partition left, Partition right) {
        if (!left.getParentPartitionId().equals(parentId)
                || !right.getParentPartitionId().equals(parentId)) {
            throw new IllegalStateException("Parent partition does not match");
        }
        Object splitPoint = singleFieldRange(left).getMax();
        if (!splitPoint.equals(singleFieldRange(right).getMin())) {
            throw new IllegalStateException(
                    "Left and right partition are mismatched (expected split point " + splitPoint + ")");
        }
        return splitPoint;
    }

    private Range singleFieldRange(Partition partition) {
        return partition.getRegion().getRange(KEY_FIELD);
    }

}
