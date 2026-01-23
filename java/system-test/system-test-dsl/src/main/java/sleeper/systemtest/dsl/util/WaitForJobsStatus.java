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
import java.util.List;
import java.util.Map;
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

    public static WaitForJobsStatus fromJobs(
            Stream<JobStatus<?>> jobs, int numJobs, Instant now) {
        Builder builder = new Builder(now);
        jobs.forEach(builder::addJob);
        builder.reportRemainingHaveNoStatus(numJobs);
        return builder.build();
    }

    public static WaitForJobsStatus fromJobs(
            Stream<JobStatus<?>> jobs, Instant now) {
        Builder builder = new Builder(now);
        AtomicInteger numJobs = new AtomicInteger();
        jobs.forEach(job -> {
            numJobs.incrementAndGet();
            builder.addJob(job);
        });
        builder.reportRemainingHaveNoStatus(numJobs.get());
        return builder.build();
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
        private Integer numUnstarted;
        private int numUnfinished;
        private Instant firstInProgressStartTime;
        private Duration longestInProgressDuration;
        private final Instant now;

        private Builder(Instant now) {
            this.now = now;
        }

        public <T extends JobRunReport> void addJob(JobStatus<T> status) {
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
                    List<String> runFailureReasons = run.getFailureReasons();
                    for (int i = 0;
                         i < runFailureReasons.size()
                                 && failureReasons.size() < 10;
                         i++) {
                        failureReasons.add(runFailureReasons.get(i));
                    }
                }
                numUnfinished++;
            }
            countByFurthestStatus.compute(status.furthestStatusType,
                    (key, value) -> value == null ? 1 : value + 1);
        }

        public void reportRemainingHaveNoStatus(int numJobs) {
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
