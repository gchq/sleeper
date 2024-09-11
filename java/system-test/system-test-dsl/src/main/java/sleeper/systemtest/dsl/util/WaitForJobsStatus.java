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

package sleeper.systemtest.dsl.util;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusType;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.util.GsonConfig;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusType;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class WaitForJobsStatus {

    private static final Gson GSON = GsonConfig.standardBuilder().setPrettyPrinting()
            .registerTypeAdapter(Duration.class, durationSerializer())
            .registerTypeAdapter(Instant.class, instantSerializer())
            .create();

    private final Map<String, Integer> countByFurthestStatus;
    private final Integer numUnstarted;
    private final int numUnfinished;
    private final Instant firstInProgressStartTime;
    private final Duration longestInProgressDuration;

    private WaitForJobsStatus(Builder builder) {
        countByFurthestStatus = builder.countByFurthestStatus;
        numUnstarted = builder.numUnstarted;
        numUnfinished = builder.numUnfinished;
        firstInProgressStartTime = builder.firstInProgressStartTime;
        longestInProgressDuration = builder.longestInProgressDuration;
    }

    public static WaitForJobsStatus forIngest(IngestJobStatusStore store, Collection<String> jobIds, Instant now) {
        return forJobStore(jobId -> store.getJob(jobId).map(JobStatus::new), jobIds, now);
    }

    public static WaitForJobsStatus forCompaction(CompactionJobStatusStore store, Collection<String> jobIds, Instant now) {
        return forJobStore(jobId -> store.getJob(jobId).map(JobStatus::new), jobIds, now);
    }

    public boolean areAllJobsFinished() {
        return numUnfinished == 0;
    }

    public String toString() {
        return GSON.toJson(this);
    }

    private static WaitForJobsStatus forJobStore(
            JobStatusStore store,
            Collection<String> jobIds, Instant now) {
        Builder builder = new Builder(now);
        jobIds.stream().parallel()
                .map(jobId -> store.getJob(jobId)
                        .orElseGet(JobStatus::none))
                .collect(Collectors.toUnmodifiableList())
                .forEach(builder::addJob);
        return builder.build();
    }

    private static JsonSerializer<Instant> instantSerializer() {
        return (instant, type, context) -> new JsonPrimitive(instant.toString());
    }

    private static JsonSerializer<Duration> durationSerializer() {
        return (duration, type, context) -> new JsonPrimitive(duration.toString());
    }

    private interface JobStatusStore {
        Optional<JobStatus> getJob(String jobId);
    }

    private static class JobStatus {
        private final List<ProcessRun> runsLatestFirst;
        private final String furthestStatusType;
        private final boolean finished;
        private final Predicate<ProcessRun> isRunFinished;

        JobStatus(IngestJobStatus status) {
            IngestJobStatusType statusType = status.getFurthestRunStatusType();
            this.runsLatestFirst = status.getJobRuns();
            this.furthestStatusType = statusType.toString();
            this.finished = statusType == IngestJobStatusType.FINISHED;
            this.isRunFinished = run -> IngestJobStatusType.statusTypeOfJobRun(run) == IngestJobStatusType.FINISHED;
        }

        JobStatus(CompactionJobStatus status) {
            CompactionJobStatusType statusType = status.getFurthestStatusType();
            this.runsLatestFirst = status.getJobRuns();
            this.furthestStatusType = statusType.toString();
            this.finished = statusType == CompactionJobStatusType.FINISHED;
            this.isRunFinished = run -> CompactionJobStatusType.statusTypeOfJobRun(run) == CompactionJobStatusType.FINISHED;
        }

        JobStatus() {
            runsLatestFirst = List.of();
            furthestStatusType = "NONE";
            finished = false;
            isRunFinished = run -> false;
        }

        static JobStatus none() {
            return new JobStatus();
        }
    }

    public static final class Builder {
        private final Map<String, Integer> countByFurthestStatus = new TreeMap<>();
        private Integer numUnstarted;
        private int numUnfinished;
        private Instant firstInProgressStartTime;
        private Duration longestInProgressDuration;
        private final Instant now;

        private Builder(Instant now) {
            this.now = now;
        }

        public void addJob(JobStatus status) {
            List<ProcessRun> runsLatestFirst = status.runsLatestFirst;
            if (runsLatestFirst.isEmpty()) {
                numUnstarted = numUnstarted == null ? 1 : numUnstarted + 1;
                numUnfinished++;
            } else if (!status.finished) {
                for (ProcessRun run : runsLatestFirst) {
                    if (status.isRunFinished.test(run)) {
                        continue;
                    }
                    Instant startTime = run.getStartTime();
                    if (firstInProgressStartTime == null || startTime.isBefore(firstInProgressStartTime)) {
                        firstInProgressStartTime = startTime;
                        longestInProgressDuration = Duration.between(startTime, now);
                    }
                }
                numUnfinished++;
            }
            countByFurthestStatus.compute(status.furthestStatusType,
                    (key, value) -> value == null ? 1 : value + 1);
        }

        public WaitForJobsStatus build() {
            return new WaitForJobsStatus(this);
        }
    }
}
