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
package sleeper.core.tracker.job.status;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Updates held in a job tracker for a job. These are sorted by the latest update first, and are organised by
 * correlating updates that occur in the same run of a job. The raw records from the job tracker are also kept.
 */
public class JobStatusUpdates {

    private final String jobId;
    private final List<ProcessStatusUpdateRecord> recordsLatestFirst;
    private final ProcessRuns runs;

    private JobStatusUpdates(
            String jobId, List<ProcessStatusUpdateRecord> recordsLatestFirst, ProcessRuns runs) {
        this.jobId = jobId;
        this.recordsLatestFirst = recordsLatestFirst;
        this.runs = runs;
    }

    /**
     * Creates an instance of this class. The records are correlated to detect which update occurs on which run of the
     * job.
     *
     * @param  jobId   the job ID to set
     * @param  records the list of status update records for the job
     * @return         an instance of this class
     */
    public static JobStatusUpdates from(String jobId, List<ProcessStatusUpdateRecord> records) {
        List<ProcessStatusUpdateRecord> recordsLatestFirst = orderLatestFirst(records);
        ProcessRuns runs = ProcessRuns.fromRecordsLatestFirst(recordsLatestFirst);
        return new JobStatusUpdates(jobId, recordsLatestFirst, runs);
    }

    /**
     * Creates and streams instances of this class from status update records. The records are gathered and organised
     * for each job.
     *
     * @param  records the list of status update records
     * @return         a stream of instances of this class, each for a different job
     */
    public static Stream<JobStatusUpdates> streamFrom(Stream<ProcessStatusUpdateRecord> records) {
        JobStatusesBuilder builder = new JobStatusesBuilder();
        records.forEach(builder::update);
        return builder.stream();
    }

    public String getJobId() {
        return jobId;
    }

    public ProcessStatusUpdateRecord getFirstRecord() {
        return recordsLatestFirst.get(recordsLatestFirst.size() - 1);
    }

    public ProcessStatusUpdateRecord getLastRecord() {
        return recordsLatestFirst.get(0);
    }

    /**
     * Gets the first status update of the provided type.
     *
     * @param  <T>        the status update type
     * @param  updateType the class defining the type of update to look for
     * @return            the first status update, or an empty optional if there is no update of the given type
     */
    public <T extends JobStatusUpdate> Optional<T> getFirstStatusUpdateOfType(Class<T> updateType) {
        for (int i = recordsLatestFirst.size() - 1; i >= 0; i--) {
            JobStatusUpdate update = recordsLatestFirst.get(i).getStatusUpdate();
            if (updateType.isInstance(update)) {
                return Optional.of(updateType.cast(update));
            }
        }
        return Optional.empty();
    }

    public ProcessRuns getRuns() {
        return runs;
    }

    private static List<ProcessStatusUpdateRecord> orderLatestFirst(List<ProcessStatusUpdateRecord> records) {
        return records.stream()
                .sorted(Comparator.comparing(ProcessStatusUpdateRecord::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }
}
