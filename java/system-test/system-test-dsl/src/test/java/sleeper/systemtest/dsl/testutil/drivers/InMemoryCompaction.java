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

import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.job.execution.StandardCompactor;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterUtils;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.WaitForJobs;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;

public class InMemoryCompaction {
    private final List<CompactionJobIdAssignmentCommitRequest> jobIdAssignmentRequests = new ArrayList<>();
    private final Map<String, CompactionJob> queuedJobsById = new TreeMap<>();
    private final List<CompactionTaskStatus> runningTasks = new ArrayList<>();
    private final CompactionJobStatusStore jobStore = new InMemoryCompactionJobStatusStore();
    private final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();
    private final InMemoryDataStore data;
    private final InMemorySketchesStore sketches;

    public InMemoryCompaction(InMemoryDataStore data, InMemorySketchesStore sketches) {
        this.data = data;
        this.sketches = sketches;
    }

    public CompactionDriver driver(SystemTestInstanceContext instance) {
        return new Driver(instance);
    }

    public WaitForJobs waitForJobs(SystemTestContext context) {
        return WaitForJobs.forCompaction(context.instance(), properties -> {
            String taskId = runningTasks.stream().map(CompactionTaskStatus::getTaskId)
                    .findFirst().orElseThrow();
            finishJobs(context.instance(), taskId);
            finishTasks();
            return jobStore;
        }, properties -> taskStore);
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
            createJobs(Mode.STRATEGY);
        }

        @Override
        public void forceCreateJobs() {
            createJobs(Mode.FORCE_ALL_FILES_AFTER_STRATEGY);
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

        public void scaleToZero() {
        }

        private void createJobs(Mode mode) {
            CreateCompactionJobs jobCreator = jobCreator(mode);
            instance.streamTableProperties().forEach(table -> {
                try {
                    jobCreator.createJobs(table);
                } catch (StateStoreException | IOException | ObjectFactoryException e) {
                    throw new RuntimeException("Failed creating compaction jobs for table " + table.getStatus(), e);
                }
            });
        }

        private CreateCompactionJobs jobCreator(Mode mode) {
            return new CreateCompactionJobs(ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                    instance.getStateStoreProvider(), jobSender(), jobStore, mode, jobIdAssignmentRequests::add);
        }
    }

    private void finishJobs(SystemTestInstanceContext instance, String taskId) {
        TablePropertiesProvider tablesProvider = instance.getTablePropertiesProvider();
        for (CompactionJob job : queuedJobsById.values()) {
            TableProperties tableProperties = tablesProvider.getById(job.getTableId());
            RecordsProcessedSummary summary = compact(job, tableProperties, instance.getStateStore(tableProperties), taskId);
            jobStore.jobStarted(compactionJobStarted(job, summary.getStartTime()).taskId(taskId).build());
            jobStore.jobFinished(compactionJobFinished(job, summary).taskId(taskId).build());
            jobStore.jobCommitted(compactionJobCommitted(job, summary.getFinishTime().plus(Duration.ofMinutes(1))).taskId(taskId).build());
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
        try {
            CompactionJobCommitter.updateStateStoreSuccess(job, recordsProcessed.getRecordsWritten(), stateStore);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        Instant finishTime = startTime.plus(Duration.ofMinutes(1));
        return new RecordsProcessedSummary(recordsProcessed, startTime, finishTime);
    }

    private static Partition getPartitionForJob(StateStore stateStore, CompactionJob job) {
        PartitionTree partitionTree = null;
        try {
            partitionTree = new PartitionTree(stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return partitionTree.getPartition(job.getPartitionId());
    }

    private RecordsProcessed mergeInputFiles(CompactionJob job, Partition partition, Schema schema) {
        List<CloseableIterator<Record>> inputIterators = job.getInputFiles().stream()
                .map(file -> new CountingIterator(file, partition.getRegion(), schema))
                .collect(toUnmodifiableList());
        CloseableIterator<Record> mergingIterator;
        try {
            mergingIterator = StandardCompactor.getMergingIterator(
                    ObjectFactory.noUserJars(), schema, job, inputIterators);
        } catch (IteratorCreationException e) {
            throw new RuntimeException(e);
        }
        Map<String, ItemsSketch> keyFieldToSketchMap = PartitionFileWriterUtils.createQuantileSketchMap(schema);
        List<Record> records = new ArrayList<>();
        mergingIterator.forEachRemaining(record -> {
            records.add(record);
            PartitionFileWriterUtils.updateQuantileSketchMap(schema, keyFieldToSketchMap, record);
        });
        data.addFile(job.getOutputFile(), records);
        sketches.addSketchForFile(job.getOutputFile(), keyFieldToSketchMap);
        return new RecordsProcessed(records.size(), inputIterators.stream()
                .map(it -> (CountingIterator) it)
                .mapToLong(it -> it.count)
                .sum());
    }

    private CreateCompactionJobs.JobSender jobSender() {
        return job -> queuedJobsById.put(job.getId(), job);
    }

    private class CountingIterator implements CloseableIterator<Record> {

        private final Iterator<Record> iterator;
        private long count = 0;

        CountingIterator(String filename, Region region, Schema schema) {
            iterator = data.streamRecords(List.of(filename))
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
