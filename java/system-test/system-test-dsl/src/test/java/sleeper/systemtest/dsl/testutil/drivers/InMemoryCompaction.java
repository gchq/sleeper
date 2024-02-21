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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.execution.CompactSortedFiles;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.PollWithRetries;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import static java.util.stream.Collectors.toUnmodifiableList;

public class InMemoryCompaction {

    private final Map<String, CompactionJob> queuedJobsById = new TreeMap<>();
    private final List<CompactionTaskStatus> runningTasks = new ArrayList<>();
    private final CompactionJobStatusStore jobStore = new InMemoryCompactionJobStatusStore();
    private final CompactionTaskStatusStore taskStore = new InMemoryCompactionTaskStatusStore();
    private final InMemoryDataStore data;

    public InMemoryCompaction(InMemoryDataStore data) {
        this.data = data;
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

    private class Driver implements CompactionDriver {

        private final SystemTestInstanceContext instance;

        Driver(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public List<String> createJobsGetIds() {
            Set<String> jobIdsBefore = jobIds();
            try {
                CreateCompactionJobs.standard(ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                        tablePropertiesProvider(instance), instance.getStateStoreProvider(), jobSender(), jobStore)
                        .createJobs();
            } catch (StateStoreException | IOException | ObjectFactoryException e) {
                throw new RuntimeException(e);
            }
            return jobIdsExcept(jobIdsBefore);
        }

        @Override
        public List<String> forceCreateJobsGetIds() {
            Set<String> jobIdsBefore = jobIds();
            try {
                CreateCompactionJobs.compactAllFiles(ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                        tablePropertiesProvider(instance), instance.getStateStoreProvider(), jobSender(), jobStore)
                        .createJobs();
            } catch (StateStoreException | IOException | ObjectFactoryException e) {
                throw new RuntimeException(e);
            }
            return jobIdsExcept(jobIdsBefore);
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
    }

    private void finishJobs(SystemTestInstanceContext instance, String taskId) {
        TablePropertiesProvider tablesProvider = tablePropertiesProvider(instance);
        for (CompactionJob job : queuedJobsById.values()) {
            TableProperties tableProperties = tablesProvider.getById(job.getTableId());
            RecordsProcessedSummary summary = compact(job, tableProperties, instance.getStateStore(tableProperties), taskId);
            jobStore.jobStarted(job, summary.getStartTime(), taskId);
            jobStore.jobFinished(job, summary, taskId);
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
        List<CloseableIterator<Record>> inputIterators = job.getInputFiles().stream()
                .map(CountingIterator::new)
                .collect(toUnmodifiableList());
        CloseableIterator<Record> mergingIterator;
        try {
            mergingIterator = CompactSortedFiles.getMergingIterator(
                    ObjectFactory.noUserJars(), tableProperties.getSchema(), job, inputIterators);
        } catch (IteratorException e) {
            throw new RuntimeException(e);
        }
        List<Record> records = new ArrayList<>();
        mergingIterator.forEachRemaining(records::add);
        data.addFile(job.getOutputFile(), records);
        try {
            CompactSortedFiles.updateStateStoreSuccess(job, records.size(), stateStore);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        Instant finishTime = startTime.plus(Duration.ofMinutes(1));
        long recordsRead = inputIterators.stream()
                .map(it -> (CountingIterator) it)
                .mapToLong(it -> it.count)
                .sum();
        RecordsProcessed recordsProcessed = new RecordsProcessed(recordsRead, records.size());
        return new RecordsProcessedSummary(recordsProcessed, startTime, finishTime);
    }

    private static TablePropertiesProvider tablePropertiesProvider(SystemTestInstanceContext instance) {
        return new FixedTablePropertiesProvider(
                instance.streamTableProperties().collect(toUnmodifiableList()));
    }

    private CreateCompactionJobs.JobSender jobSender() {
        return job -> queuedJobsById.put(job.getId(), job);
    }

    private Set<String> jobIds() {
        return new TreeSet<>(queuedJobsById.keySet());
    }

    private List<String> jobIdsExcept(Set<String> jobIdsBefore) {
        Set<String> jobIds = jobIds();
        jobIds.removeAll(jobIdsBefore);
        return new ArrayList<>(jobIds);
    }

    private class CountingIterator implements CloseableIterator<Record> {

        private final Iterator<Record> iterator;
        private long count = 0;

        CountingIterator(String filename) {
            iterator = data.streamRecords(List.of(filename)).iterator();
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
