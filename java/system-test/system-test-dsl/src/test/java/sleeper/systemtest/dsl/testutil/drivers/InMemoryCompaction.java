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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.commit.CompactionJobCommitter;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateBatchId;
import sleeper.compaction.core.job.creation.CreateCompactionJobs.GenerateJobId;
import sleeper.compaction.core.task.CompactionTaskFinishedStatus;
import sleeper.compaction.core.task.CompactionTaskStatus;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.core.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.core.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.compaction.job.execution.JavaCompactionRunner;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.core.util.PollWithRetries;
import sleeper.query.core.recordretrieval.InMemoryDataStore;
import sleeper.sketches.Sketches;
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
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.compaction.core.job.status.CompactionJobFinishedEvent.compactionJobFinished;

public class InMemoryCompaction {
    private final List<CompactionJobIdAssignmentCommitRequest> jobIdAssignmentRequests = new ArrayList<>();
    private final Map<String, CompactionJob> queuedJobsById = new TreeMap<>();
    private final List<CompactionTaskStatus> runningTasks = new ArrayList<>();
    private final CompactionJobStatusStore jobStore = new InMemoryCompactionJobStatusStore();
    private final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();
    private final InMemoryDataStore dataStore;
    private final InMemorySketchesStore sketchesStore;

    public InMemoryCompaction(InMemoryDataStore dataStore, InMemorySketchesStore sketchesStore) {
        this.dataStore = dataStore;
        this.sketchesStore = sketchesStore;
    }

    public CompactionDriver driver(SystemTestInstanceContext instance) {
        return new Driver(instance);
    }

    public WaitForJobs waitForJobs(SystemTestContext context, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forCompaction(context.instance(), properties -> {
            String taskId = runningTasks.stream().map(CompactionTaskStatus::getTaskId)
                    .findFirst().orElseThrow();
            finishJobs(context.instance(), taskId);
            finishTasks();
            return jobStore;
        }, properties -> taskStore, pollDriver);
    }

    public CompactionJobStatusStore jobStore() {
        return jobStore;
    }

    public CompactionTaskStatusStore taskStore() {
        return taskStore;
    }

    private class Driver implements CompactionDriver {

        private final SystemTestInstanceContext instance;

        Driver(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public CompactionJobStatusStore getJobStatusStore() {
            return jobStore;
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
        public void invokeTasks(int expectedTasks, PollWithRetries poll) {
            for (int i = 0; i < expectedTasks; i++) {
                CompactionTaskStatus task = CompactionTaskStatus.builder()
                        .taskId(UUID.randomUUID().toString())
                        .startTime(Instant.now())
                        .build();
                taskStore.taskStarted(task);
                runningTasks.add(task);
            }
        }

        @Override
        public void forceStartTasks(int numberOfTasks, PollWithRetries poll) {
            invokeTasks(numberOfTasks, poll);
        }

        @Override
        public void scaleToZero() {
        }

        @Override
        public List<CompactionJob> drainJobsQueueForWholeInstance() {
            List<CompactionJob> jobs = queuedJobsById.values().stream().toList();
            queuedJobsById.clear();
            return jobs;
        }

        private CreateCompactionJobs jobCreator() {
            return new CreateCompactionJobs(ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                    instance.getStateStoreProvider(), batchJobsWriter(),
                    message -> {
                    }, jobIdAssignmentRequests::add,
                    GenerateJobId.random(), GenerateBatchId.random(), new Random(), Instant::now);
        }
    }

    private void finishJobs(SystemTestInstanceContext instance, String taskId) {
        TablePropertiesProvider tablesProvider = instance.getTablePropertiesProvider();
        for (CompactionJob job : queuedJobsById.values()) {
            TableProperties tableProperties = tablesProvider.getById(job.getTableId());
            RecordsProcessedSummary summary = compact(job, tableProperties, instance.getStateStore(tableProperties), taskId);
            jobStore.jobStarted(job.startedEventBuilder(summary.getStartTime()).taskId(taskId).build());
            jobStore.jobFinished(compactionJobFinished(job, summary).taskId(taskId).build());
            jobStore.jobCommitted(job.committedEventBuilder(summary.getFinishTime().plus(Duration.ofMinutes(1))).taskId(taskId).build());
        }
        queuedJobsById.clear();
    }

    private void finishTasks() {
        for (CompactionTaskStatus task : runningTasks) {
            taskStore.taskFinished(CompactionTaskStatus.builder()
                    .taskId(task.getTaskId())
                    .startTime(task.getStartTime())
                    .finished(task.getStartTime().plus(Duration.ofMinutes(2)), CompactionTaskFinishedStatus.builder())
                    .build());
        }
        runningTasks.clear();
    }

    private RecordsProcessedSummary compact(CompactionJob job, TableProperties tableProperties, StateStore stateStore, String taskId) {
        Instant startTime = Instant.now();
        Schema schema = tableProperties.getSchema();
        Partition partition = getPartitionForJob(stateStore, job);
        RecordsProcessed recordsProcessed = mergeInputFiles(job, partition, schema);
        CompactionJobCommitter.updateStateStoreSuccess(job, recordsProcessed.getRecordsWritten(), stateStore);
        Instant finishTime = startTime.plus(Duration.ofMinutes(1));
        return new RecordsProcessedSummary(recordsProcessed, startTime, finishTime);
    }

    private static Partition getPartitionForJob(StateStore stateStore, CompactionJob job) {
        PartitionTree partitionTree = new PartitionTree(stateStore.getAllPartitions());
        return partitionTree.getPartition(job.getPartitionId());
    }

    private RecordsProcessed mergeInputFiles(CompactionJob job, Partition partition, Schema schema) {
        List<CloseableIterator<Record>> inputIterators = job.getInputFiles().stream()
                .map(file -> new CountingIterator(file, partition.getRegion(), schema))
                .collect(toUnmodifiableList());
        CloseableIterator<Record> mergingIterator;
        try {
            mergingIterator = JavaCompactionRunner.getMergingIterator(
                    ObjectFactory.noUserJars(), schema, job, inputIterators);
        } catch (IteratorCreationException e) {
            throw new RuntimeException(e);
        }
        Sketches sketches = Sketches.from(schema);
        List<Record> records = new ArrayList<>();
        mergingIterator.forEachRemaining(record -> {
            records.add(record);
            sketches.update(record);
        });
        dataStore.addFile(job.getOutputFile(), records);
        sketchesStore.addSketchForFile(job.getOutputFile(), sketches);
        return new RecordsProcessed(records.size(), inputIterators.stream()
                .map(it -> (CountingIterator) it)
                .mapToLong(it -> it.count)
                .sum());
    }

    private CreateCompactionJobs.BatchJobsWriter batchJobsWriter() {
        return (bucketName, key, jobs) -> jobs.forEach(
                job -> {
                    queuedJobsById.put(job.getId(), job);
                    jobStore.jobCreated(job.createCreatedEvent());
                });
    }

    private class CountingIterator implements CloseableIterator<Record> {

        private final Iterator<Record> iterator;
        private long count = 0;

        CountingIterator(String filename, Region region, Schema schema) {
            iterator = dataStore.streamRecords(List.of(filename))
                    .filter(record -> region.isKeyInRegion(schema, record.getRowKeys(schema)))
                    .iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Record next() {
            Record record = iterator.next();
            count++;
            return record;
        }

        @Override
        public void close() throws IOException {
        }

    }

}
