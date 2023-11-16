/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.util;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.clients.util.GsonConfig;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class WaitForJobsStatus {

    private static final Gson GSON = GsonConfig.standardBuilder()
            .registerTypeAdapter(Duration.class, durationSerializer())
            .create();

    private final Map<String, Integer> countByLastStatus;
    private final Integer numUnstarted;
    private final int numUnfinished;
    private final Instant firstInProgressStartTime;
    private final Duration longestInProgressDuration;

    private WaitForJobsStatus(Builder builder) {
        countByLastStatus = builder.countByLastStatus;
        numUnstarted = builder.numUnstarted;
        numUnfinished = builder.numUnfinished;
        firstInProgressStartTime = builder.firstInProgressStartTime;
        longestInProgressDuration = builder.longestInProgressDuration;
    }

    public static WaitForJobsStatus forIngest(IngestJobStatusStore store, Collection<String> jobIds, Instant now) {
        return forGeneric(store::getJob, IngestJobStatus::getJobRuns, jobIds, now);
    }

    public static WaitForJobsStatus forCompaction(CompactionJobStatusStore store, Collection<String> jobIds, Instant now) {
        return forGeneric(store::getJob, CompactionJobStatus::getJobRuns, jobIds, now);
    }

    public boolean isAllFinished() {
        return numUnfinished == 0;
    }

    public String toString() {
        return GSON.toJson(this);
    }

    private static <T> WaitForJobsStatus forGeneric(
            JobStatusStore<T> store, Function<T, List<ProcessRun>> getRuns, Collection<String> jobIds, Instant now) {
        Builder builder = new Builder(now);
        jobIds.stream().parallel()
                .map(jobId -> store.getJob(jobId)
                        .map(getRuns)
                        .orElseGet(List::of))
                .collect(Collectors.toUnmodifiableList())
                .forEach(builder::addJob);
        return builder.build();
    }

    interface JobStatusStore<T> {
        Optional<T> getJob(String jobId);
    }

    private static JsonSerializer<Duration> durationSerializer() {
        return (duration, type, context) -> new JsonPrimitive(duration.toString());
    }

    public static final class Builder {
        private final Map<String, Integer> countByLastStatus = new TreeMap<>();
        private Integer numUnstarted;
        private int numUnfinished;
        private Instant firstInProgressStartTime;
        private Duration longestInProgressDuration;
        private final Instant now;

        private Builder(Instant now) {
            this.now = now;
        }

        public void addJob(List<ProcessRun> runsLatestFirst) {
            boolean inProgress = false;
            for (ProcessRun run : runsLatestFirst) {
                if (run.isFinished()) {
                    continue;
                } else {
                    inProgress = true;
                }
                Instant startTime = run.getStartTime();
                if (firstInProgressStartTime == null || startTime.isBefore(firstInProgressStartTime)) {
                    firstInProgressStartTime = startTime;
                    longestInProgressDuration = Duration.between(startTime, now);
                }
            }
            if (inProgress) {
                numUnfinished++;
            } else if (runsLatestFirst.isEmpty()) {
                numUnstarted = numUnstarted == null ? 1 : numUnstarted + 1;
                numUnfinished++;
            }
            String status = runsLatestFirst.stream()
                    .map(ProcessRun::getLatestUpdate)
                    .map(update -> update.getClass().getSimpleName())
                    .findFirst().orElse("None");
            countByLastStatus.compute(status,
                    (key, value) -> value == null ? 1 : value + 1);
        }

        public WaitForJobsStatus build() {
            return new WaitForJobsStatus(this);
        }
    }
}
