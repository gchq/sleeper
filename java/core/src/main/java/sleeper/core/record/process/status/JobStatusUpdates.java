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
package sleeper.core.record.process.status;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stores a list of status update records and process runs for a job.
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
     * Creates an instance of this class.
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
     * Creates and streams instances of this class from status update records.
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
     * @param  <T>        the type of status update to look for
     * @param  updateType the class to get the type from
     * @return            the first status update casted to {@link T}
     */
    public <T extends ProcessStatusUpdate> Optional<T> getFirstStatusUpdateOfType(Class<T> updateType) {
        for (int i = recordsLatestFirst.size() - 1; i >= 0; i--) {
            ProcessStatusUpdate update = recordsLatestFirst.get(i).getStatusUpdate();
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
