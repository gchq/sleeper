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
import java.util.Optional;
import java.util.function.Function;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class WaitForJobsStatus {

    private static final Gson GSON = GsonConfig.standardBuilder().create();

    private final int numUnstarted;
    private final int numInProgress;
    private final int numFinished;
    private final Instant firstInProgressStartTime;
    private final Duration longestInProgressDuration;

    private WaitForJobsStatus(Builder builder) {
        numUnstarted = builder.numUnstarted;
        numInProgress = builder.numInProgress;
        numFinished = builder.numFinished;
        firstInProgressStartTime = builder.firstInProgressStartTime;
        longestInProgressDuration = builder.longestInProgressDuration;
    }

    public static WaitForJobsStatus forIngest(IngestJobStatusStore store, Collection<String> jobIds) {
        return forGeneric(store::getJob, IngestJobStatus::getJobRuns, jobIds);
    }

    public static WaitForJobsStatus forCompaction(CompactionJobStatusStore store, Collection<String> jobIds) {
        return forGeneric(store::getJob, CompactionJobStatus::getJobRuns, jobIds);
    }

    public boolean isAllFinished() {
        return numUnstarted == 0 && numInProgress == 0;
    }

    public String toString() {
        return GSON.toJson(this);
    }

    private static <T> WaitForJobsStatus forGeneric(
            JobStatusStore<T> store, Function<T, List<ProcessRun>> getRuns, Collection<String> jobIds) {
        Builder builder = new Builder();
        for (String jobId : jobIds) {
            store.getJob(jobId).ifPresentOrElse(
                    status -> builder.addJob(getRuns.apply(status)),
                    () -> builder.addJob(List.of()));
        }
        return builder.build();
    }

    interface JobStatusStore<T> {
        Optional<T> getJob(String jobId);
    }

    public static final class Builder {
        private int numUnstarted;
        private int numInProgress;
        private int numFinished;
        private Instant firstInProgressStartTime;
        private Duration longestInProgressDuration;
        private final Instant now = Instant.now();

        private Builder() {
        }

        public void addJob(List<ProcessRun> runs) {
            if (runs.isEmpty()) {
                numUnstarted++;
                return;
            }

            boolean inProgress = false;
            for (ProcessRun run : runs) {
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
                numInProgress++;
            } else {
                numFinished++;
            }
        }

        public WaitForJobsStatus build() {
            return new WaitForJobsStatus(this);
        }
    }
}
