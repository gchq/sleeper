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
package sleeper.clients.report.filestatus;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toSet;

/**
 * Collects information about files in a Sleeper table for reporting. Produces a {@link TableFilesStatus} data
 * structure.
 */
public class FileStatusCollector {
    private static final int JOB_TRACKER_LOOKUP_THREADS = 10;

    private final StateStore stateStore;
    private final CompactionJobTrackerLookup jobTracker;
    private final Supplier<Instant> timeSupplier;

    public FileStatusCollector(StateStore stateStore, CompactionJobTrackerLookup jobTracker, Supplier<Instant> timeSupplier) {
        this.stateStore = stateStore;
        this.jobTracker = jobTracker;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Retrieves data from the state store and produces a report on the status of files. Only includes information
     * requested by the detail level.
     *
     * @param  maxNumberOFilesWithNoReferencesToCount the maximum number of files that have no references to include in
     *                                                the report
     * @param  detailLevel                            the level of detail the report will include
     * @return                                        the report
     */
    public TableFilesStatus run(int maxNumberOFilesWithNoReferencesToCount, FileStatusDetailLevel detailLevel) {
        AllReferencesToAllFiles files = stateStore.getAllFilesWithMaxUnreferenced(maxNumberOFilesWithNoReferencesToCount);
        List<Partition> partitions = stateStore.getAllPartitions();

        int leafPartitionCount = partitions.stream()
                .filter(Partition::isLeafPartition)
                .mapToInt(p -> 1).sum();
        int nonLeafPartitionCount = partitions.size() - leafPartitionCount;
        PartitionTree partitionTree = new PartitionTree(partitions);
        CompactionJobTrackerLookup jobTrackerLookup = lookUpJobs(files, detailLevel);
        List<TableFilesPartitionCompactions> compactions = TableFilesPartitionCompactions.from(files, partitionTree, jobTrackerLookup);
        Instant now = timeSupplier.get();

        return TableFilesStatus.builder()
                .timeOfReport(now)
                .leafPartitionCount(leafPartitionCount)
                .nonLeafPartitionCount(nonLeafPartitionCount)
                .statistics(TableFilesStatistics.from(now, files, partitionTree, compactions, jobTrackerLookup.isTrackerAvailable()))
                .moreThanMax(files.isMoreThanMax())
                .files(detailLevel.isShowFileReferences() ? files : null)
                .compactions(detailLevel.isShowCompactionJobs() ? compactions : null)
                .build();
    }

    private CompactionJobTrackerLookup lookUpJobs(AllReferencesToAllFiles files, FileStatusDetailLevel detailLevel) {
        if (!detailLevel.isShowCompactionJobs() || !jobTracker.isTrackerAvailable()) {
            return CompactionJobTrackerLookup.none();
        }
        Set<String> jobIds = files.streamFileReferences()
                .map(FileReference::getJobId)
                .filter(Objects::nonNull)
                .collect(toSet());
        if (jobIds.isEmpty()) {
            return jobId -> Optional.empty();
        }
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(jobIds.size(), JOB_TRACKER_LOOKUP_THREADS));
        try {
            List<CompletableFuture<Optional<CompactionJobStatus>>> lookups = jobIds.stream()
                    .map(jobId -> CompletableFuture.supplyAsync(() -> jobTracker.getJob(jobId), executor))
                    .toList();
            Map<String, CompactionJobStatus> statusByJobId = new HashMap<>();
            for (CompletableFuture<Optional<CompactionJobStatus>> lookup : lookups) {
                Optional<CompactionJobStatus> status = join(lookup);
                status.ifPresent(found -> statusByJobId.put(found.getJobId(), found));
            }
            return jobId -> Optional.ofNullable(statusByJobId.get(jobId));
        } finally {
            executor.shutdownNow();
        }
    }

    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw e;
        }
    }

}
