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

package sleeper.systemtest.dsl.util;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobRun;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatusType;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobRun;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatusType;
import sleeper.core.tracker.job.run.JobRunReport;
import sleeper.core.util.GsonConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;

@SuppressFBWarnings("URF_UNREAD_FIELD") // Fields are read by GSON
public class WaitForJobsStatus {

    private static final Gson GSON = GsonConfig.standardBuilder().setPrettyPrinting()
            .registerTypeAdapter(Duration.class, durationSerializer())
            .registerTypeAdapter(Instant.class, instantSerializer())
            .create();

    private final Map<String, Integer> countByFurthestStatus;
    private final List<String> failureReasons;
    private final Integer numUnstarted;
    private final int numUnfinished;
    private final Instant firstInProgressStartTime;
    private final Duration longestInProgressDuration;

    private WaitForJobsStatus(Builder builder) {
        countByFurthestStatus = builder.countByFurthestStatus;
        failureReasons = builder.failureReasons.isEmpty() ? null : builder.failureReasons;
        numUnstarted = builder.numUnstarted;
        numUnfinished = builder.numUnfinished;
        firstInProgressStartTime = builder.firstInProgressStartTime;
        longestInProgressDuration = builder.longestInProgressDuration;
    }

    public static Builder atTime(Instant now) {
        return new Builder(now);
    }

    public static Stream<JobStatus<?>> streamIngestJobs(IngestJobTracker tracker, Collection<TableProperties> tables) {
        return tables.stream().flatMap(table -> tracker
                .streamAllJobs(table.get(TABLE_ID))
                .map(JobStatus::ingest));
    }

    public static Stream<JobStatus<?>> streamCompactionJobs(CompactionJobTracker tracker, Collection<TableProperties> tables) {
        return tables.stream().flatMap(table -> tracker
                .streamAllJobs(table.get(TABLE_ID))
                .map(JobStatus::compaction));
    }

    public boolean areAllJobsFinished() {
        return numUnfinished == 0;
    }

    public String toString() {
        return GSON.toJson(this);
    }

    private static JsonSerializer<Instant> instantSerializer() {
        return (instant, type, context) -> new JsonPrimitive(instant.toString());
    }

    private static JsonSerializer<Duration> durationSerializer() {
        return (duration, type, context) -> new JsonPrimitive(duration.toString());
    }

    public static class JobStatus<T extends JobRunReport> {
        private final String jobId;
        private final List<T> runsLatestFirst;
        private final String furthestStatusType;
        private final boolean finished;
        private final Predicate<T> isRunFinished;

        private JobStatus(String jobId, List<T> runsLatestFirst, String furthestStatusType, boolean finished, Predicate<T> isRunFinished) {
            this.jobId = jobId;
            this.runsLatestFirst = runsLatestFirst;
            this.furthestStatusType = furthestStatusType;
            this.finished = finished;
            this.isRunFinished = isRunFinished;
        }

        public static JobStatus<IngestJobRun> ingest(IngestJobStatus status) {
            IngestJobStatusType statusType = status.getFurthestRunStatusType();
            return new JobStatus<>(status.getJobId(), status.getRunsLatestFirst(), statusType.toString(), statusType == IngestJobStatusType.FINISHED,
                    run -> run.getStatusType() == IngestJobStatusType.FINISHED);
        }

        public static JobStatus<CompactionJobRun> compaction(CompactionJobStatus status) {
            CompactionJobStatusType statusType = status.getFurthestStatusType();
            return new JobStatus<>(status.getJobId(), status.getRunsLatestFirst(), statusType.toString(), statusType == CompactionJobStatusType.FINISHED,
                    run -> run.getStatusType() == CompactionJobStatusType.FINISHED);
        }

        String getJobId() {
            return jobId;
        }
    }

    public static final class Builder {
        private final Map<String, Integer> countByFurthestStatus = new TreeMap<>();
        private final List<String> failureReasons = new ArrayList<>();
        private int maxFailureReasons = 10;
        private Integer numUnstarted;
        private int numUnfinished;
        private Instant firstInProgressStartTime;
        private Duration longestInProgressDuration;
        private final Instant now;

        private Builder(Instant now) {
            this.now = now;
        }

        public Builder maxFailureReasons(int maxFailureReasons) {
            this.maxFailureReasons = maxFailureReasons;
            return this;
        }

        public Builder reportById(Collection<String> jobIds, Stream<JobStatus<?>> jobs) {
            Set<String> jobIdsSet = new HashSet<>(jobIds);
            Stream<JobStatus<?>> statuses = jobs
                    .filter(job -> jobIdsSet.contains(job.getJobId()));
            return reportSized(jobIds.size(), statuses);
        }

        public Builder report(Stream<JobStatus<?>> jobs) {
            AtomicInteger numJobs = new AtomicInteger();
            jobs.forEach(job -> {
                numJobs.incrementAndGet();
                addJob(job);
            });
            reportRemainingHaveNoStatus(numJobs.get());
            return this;
        }

        private Builder reportSized(int numJobs, Stream<JobStatus<?>> jobs) {
            jobs.forEach(this::addJob);
            reportRemainingHaveNoStatus(numJobs);
            return this;
        }

        private <T extends JobRunReport> void addJob(JobStatus<T> status) {
            List<T> runsLatestFirst = status.runsLatestFirst;
            if (runsLatestFirst.isEmpty()) {
                numUnstarted = numUnstarted == null ? 1 : numUnstarted + 1;
                numUnfinished++;
            } else if (!status.finished) {
                for (T run : runsLatestFirst) {
                    if (status.isRunFinished.test(run)) {
                        continue;
                    }
                    Instant startTime = run.getStartTime();
                    if (firstInProgressStartTime == null || startTime.isBefore(firstInProgressStartTime)) {
                        firstInProgressStartTime = startTime;
                        longestInProgressDuration = Duration.between(startTime, now);
                    }
                    addFailureReasons(run.getFailureReasons());
                }
                numUnfinished++;
            }
            countByFurthestStatus.compute(status.furthestStatusType,
                    (key, value) -> value == null ? 1 : value + 1);
        }

        private void addFailureReasons(List<String> runFailureReasons) {
            int prevCount = failureReasons.size();
            if (prevCount >= maxFailureReasons) {
                return;
            }
            int addCount = Math.max(maxFailureReasons - prevCount, runFailureReasons.size());
            List<String> toAdd = runFailureReasons.subList(0, addCount);
            failureReasons.addAll(toAdd);
        }

        private void reportRemainingHaveNoStatus(int numJobs) {
            int totalReported = countByFurthestStatus.values().stream().mapToInt(count -> count).sum();
            int numUnreported = numJobs - totalReported;
            if (numUnreported > 0) {
                countByFurthestStatus.put("NONE", numUnreported);
                numUnstarted = numUnstarted == null ? numUnreported : numUnstarted + numUnreported;
                numUnfinished += numUnreported;
            }
        }

        public WaitForJobsStatus build() {
            return new WaitForJobsStatus(this);
        }
    }
}
