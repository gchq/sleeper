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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateBatchId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateJobId;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.job.execution.JavaCompactionRunner;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.compaction.task.InMemoryCompactionTaskTracker;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.InMemorySketchesStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class InMemoryCompaction {
    private final List<CompactionJob> queuedJobs = new ArrayList<>();
    private final List<CompactionTaskStatus> runningTasks = new ArrayList<>();
    private final CompactionJobTracker jobTracker = new InMemoryCompactionJobTracker();
    private final CompactionTaskTracker taskTracker = new InMemoryCompactionTaskTracker();
    private final InMemoryRowStore dataStore;
    private final InMemorySketchesStore sketchesStore;

    public InMemoryCompaction(InMemoryRowStore dataStore, InMemorySketchesStore sketchesStore) {
        this.dataStore = dataStore;
        this.sketchesStore = sketchesStore;
    }

    public CompactionDriver driver(SystemTestInstanceContext instance) {
        return new Driver(instance);
    }

    public WaitForJobs waitForJobs(SystemTestContext context, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forCompaction(context.instance(), properties -> {
            finishJobs(context.instance());
            finishTasks();
            return jobTracker;
        }, properties -> taskTracker, pollDriver);
    }

    public CompactionJobTracker jobTracker() {
        return jobTracker;
    }

    public CompactionTaskTracker taskTracker() {
        return taskTracker;
    }

    private class Driver implements CompactionDriver {

        private final SystemTestInstanceContext instance;

        Driver(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public CompactionJobTracker getJobTracker() {
            return jobTracker;
        }

        @Override
        public void triggerCreateJobs() {
            CreateCompactionJobs jobCreator = jobCreator();
            instance.streamTableProperties().forEach(table -> {
                try {
                    jobCreator.createJobsWithStrategy(table);
                } catch (IOException | ObjectFactoryException e) {
                    throw new RuntimeException("Failed creating compaction jobs for table " + table.getStatus(), e);
                }
            });
        }

        @Override
        public void forceCreateJobs() {
            CreateCompactionJobs jobCreator = jobCreator();
            instance.streamTableProperties().forEach(table -> {
                try {
                    jobCreator.createJobWithForceAllFiles(table);
                } catch (IOException | ObjectFactoryException e) {
                    throw new RuntimeException("Failed creating compaction jobs for table " + table.getStatus(), e);
                }
            });
        }

        @Override
        public void scaleToZero() {
        }

        @Override
        public List<CompactionJob> drainJobsQueueForWholeInstance(int expectedJobs) {
            List<CompactionJob> jobs = new ArrayList<>(queuedJobs);
            queuedJobs.clear();
            return jobs;
        }

        @Override
        public void sendCompactionCommits(Stream<CompactionCommitMessage> commits) {
            TablePropertiesProvider tablesProvider = instance.getTablePropertiesProvider();
            commits.forEach(commit -> {
                ReplaceFileReferencesRequest request = commit.request();
                TableProperties tableProperties = tablesProvider.getById(commit.tableId());
                update(instance.getStateStore(tableProperties)).atomicallyReplaceFileReferencesWithNewOnes(List.of(request));
                if (request.getJobRunId() != null) {
                    jobTracker.jobCommitted(CompactionJobCommittedEvent.builder()
                            .jobId(request.getJobId())
                            .tableId(commit.tableId())
                            .taskId(request.getTaskId())
                            .jobRunId(request.getJobRunId())
                            .commitTime(Instant.now())
                            .build());
                }
            });
        }

        @Override
        public String sendCompactionBatchGetKey(List<CompactionJob> jobs) {
            throw new UnsupportedOperationException("Compaction batches are not used in memory");
        }

        @Override
        public List<CompactionJobDispatchRequest> drainPendingDeadLetterQueueForWholeInstance() {
            throw new UnsupportedOperationException("Pending compactions are not used in memory");
        }

        private CreateCompactionJobs jobCreator() {
            return new CreateCompactionJobs(ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                    instance.getStateStoreProvider(), batchJobsWriter(),
                    compactionJobMessage -> {
                    }, idAssignmentCommit -> {
                    },
                    GenerateJobId.random(), GenerateBatchId.random(), new Random(), Instant::now);
        }
    }

    private void finishJobs(SystemTestInstanceContext instance) {
        TablePropertiesProvider tablesProvider = instance.getTablePropertiesProvider();
        for (CompactionJob job : queuedJobs) {
            TableProperties tableProperties = tablesProvider.getById(job.getTableId());
            CompactionJobStatus status = jobTracker.getJob(job.getId()).orElseThrow();
            JobRunReport run = status.getRunsLatestFirst().stream().findFirst().orElseThrow();
            JobRunSummary summary = compact(job, tableProperties, instance.getStateStore(tableProperties), run);
            jobTracker.jobFinished(job.finishedEventBuilder(summary).taskId(run.getTaskId()).jobRunId(job.getId()).build());
            jobTracker.jobCommitted(job.committedEventBuilder(summary.getFinishTime().plus(Duration.ofMinutes(1))).taskId(run.getTaskId()).jobRunId(job.getId()).build());
        }
        queuedJobs.clear();
    }

    private void finishTasks() {
        for (CompactionTaskStatus task : runningTasks) {
            taskTracker.taskFinished(CompactionTaskStatus.builder()
                    .taskId(task.getTaskId())
                    .startTime(task.getStartTime())
                    .finished(task.getStartTime().plus(Duration.ofMinutes(2)), CompactionTaskFinishedStatus.builder())
                    .build());
        }
        runningTasks.clear();
    }

    private JobRunSummary compact(CompactionJob job, TableProperties tableProperties, StateStore stateStore, JobRunReport run) {
        Instant startTime = run.getStartTime();
        Schema schema = tableProperties.getSchema();
        Partition partition = getPartitionForJob(stateStore, job);
        RowsProcessed rowsProcessed = mergeInputFiles(job, partition, schema);
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(
                job.replaceFileReferencesRequestBuilder(rowsProcessed.getRowsWritten())
                        .taskId(run.getTaskId()).jobRunId(job.getId()).build()));
        Instant finishTime = startTime.plus(Duration.ofMinutes(1));
        return new JobRunSummary(rowsProcessed, startTime, finishTime);
    }

    private static Partition getPartitionForJob(StateStore stateStore, CompactionJob job) {
        PartitionTree partitionTree = new PartitionTree(stateStore.getAllPartitions());
        return partitionTree.getPartition(job.getPartitionId());
    }

    private RowsProcessed mergeInputFiles(CompactionJob job, Partition partition, Schema schema) {
        List<CloseableIterator<Row>> inputIterators = job.getInputFiles().stream()
                .map(file -> new CountingIterator(file, partition.getRegion(), schema))
                .collect(toUnmodifiableList());
        CloseableIterator<Row> mergingIterator;
        try {
            mergingIterator = JavaCompactionRunner.getMergingIterator(
                    ObjectFactory.noUserJars(), schema, job, inputIterators);
        } catch (IteratorCreationException e) {
            throw new RuntimeException(e);
        }
        Sketches sketches = Sketches.from(schema);
        List<Row> rows = new ArrayList<>();
        mergingIterator.forEachRemaining(row -> {
            rows.add(row);
            sketches.update(row);
        });
        dataStore.addFile(job.getOutputFile(), rows);
        sketchesStore.saveFileSketches(job.getOutputFile(), sketches);
        return new RowsProcessed(rows.size(), inputIterators.stream()
                .map(it -> (CountingIterator) it)
                .mapToLong(it -> it.count)
                .sum());
    }

    private CreateCompactionJobs.BatchJobsWriter batchJobsWriter() {
        return (bucketName, key, jobs) -> jobs.forEach(job -> {
            queuedJobs.add(job);
            jobTracker.jobCreated(job.createCreatedEvent());
            CompactionTaskStatus task = CompactionTaskStatus.builder()
                    .taskId(UUID.randomUUID().toString())
                    .startTime(Instant.now())
                    .build();
            taskTracker.taskStarted(task);
            runningTasks.add(task);
            jobTracker.jobStarted(job.startedEventBuilder(Instant.now()).taskId(task.getTaskId()).jobRunId(job.getId()).build());
        });
    }

    private class CountingIterator implements CloseableIterator<Row> {

        private final Iterator<Row> iterator;
        private long count = 0;

        CountingIterator(String filename, Region region, Schema schema) {
            iterator = dataStore.streamRows(List.of(filename))
                    .filter(row -> region.isKeyInRegion(schema, row.getRowKeys(schema)))
                    .iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            Row row = iterator.next();
            count++;
            return row;
        }

        @Override
        public void close() throws IOException {
        }

    }

}
