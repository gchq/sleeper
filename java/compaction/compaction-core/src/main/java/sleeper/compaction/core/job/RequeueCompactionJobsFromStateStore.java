/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.compaction.core.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher.SendJobs;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.SplitIntoBatches;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Recreates compaction jobs from the job IDs assigned to files in the state store, and sends them to the compaction
 * job queue. This is used to recover when files have been assigned to compaction jobs that did not succeed, which would
 * otherwise leave those files permanently assigned to a job that will never be run.
 */
public class RequeueCompactionJobsFromStateStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(RequeueCompactionJobsFromStateStore.class);

    private static final int SEND_BATCH_SIZE = 10;

    private final InstanceProperties instanceProperties;
    private final SendJobs sendJobs;
    private final CompactionJobTracker tracker;

    public RequeueCompactionJobsFromStateStore(InstanceProperties instanceProperties, SendJobs sendJobs, CompactionJobTracker tracker) {
        this.instanceProperties = instanceProperties;
        this.sendJobs = sendJobs;
        this.tracker = tracker;
    }

    /**
     * Recreates all compaction jobs that files in the given table are assigned to, and sends them to the queue.
     *
     * @param  tableProperties the table properties
     * @param  stateStore      the state store for the table
     * @return                 the compaction jobs that were sent
     */
    public List<CompactionJob> requeue(TableProperties tableProperties, StateStore stateStore) {
        List<CompactionJob> jobs = createJobs(tableProperties, stateStore);
        LOGGER.info("Found {} compaction jobs assigned to files in table {}", jobs.size(), tableProperties.getStatus());
        for (List<CompactionJob> batch : SplitIntoBatches.splitListIntoBatchesOf(SEND_BATCH_SIZE, jobs)) {
            sendJobs.send(batch);
            batch.forEach(job -> tracker.jobCreated(job.createCreatedEvent()));
        }
        LOGGER.info("Requeued {} compaction jobs for table {}", jobs.size(), tableProperties.getStatus());
        return jobs;
    }

    private List<CompactionJob> createJobs(TableProperties tableProperties, StateStore stateStore) {
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        Map<String, List<FileReference>> referencesByJobId = stateStore.getFileReferences().stream()
                .filter(reference -> reference.getJobId() != null)
                .collect(Collectors.groupingBy(FileReference::getJobId, LinkedHashMap::new, Collectors.toList()));
        return referencesByJobId.entrySet().stream()
                .flatMap(entry -> createJob(factory, entry.getKey(), entry.getValue()).stream())
                .toList();
    }

    private static Optional<CompactionJob> createJob(CompactionJobFactory factory, String jobId, List<FileReference> references) {
        Set<String> partitionIds = references.stream()
                .map(FileReference::getPartitionId)
                .collect(Collectors.toSet());
        if (partitionIds.size() != 1) {
            LOGGER.error("Found file references assigned to compaction job {} in multiple partitions, skipping job. Partitions: {}",
                    jobId, partitionIds);
            return Optional.empty();
        }
        return Optional.of(factory.createCompactionJob(jobId, references, references.get(0).getPartitionId()));
    }
}
