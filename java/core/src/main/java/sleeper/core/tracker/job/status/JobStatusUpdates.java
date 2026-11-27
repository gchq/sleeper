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
package sleeper.core.tracker.job.status;

import sleeper.core.tracker.job.run.JobRuns;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Updates held in a job tracker for a job. These are sorted by the latest update first, and are organised by
 * correlating updates that occur in the same run of a job. The raw records from the job tracker are also kept.
 */
public class JobStatusUpdates {

    private final String jobId;
    private final List<JobStatusUpdateRecord> recordsLatestFirst;
    private final JobRuns runs;

    private JobStatusUpdates(
            String jobId, List<JobStatusUpdateRecord> recordsLatestFirst, JobRuns runs) {
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
    public static JobStatusUpdates from(String jobId, List<JobStatusUpdateRecord> records) {
        List<JobStatusUpdateRecord> recordsLatestFirst = orderLatestFirst(records);
        JobRuns runs = JobRuns.fromRecordsLatestFirst(recordsLatestFirst);
        return new JobStatusUpdates(jobId, recordsLatestFirst, runs);
    }

    /**
     * Creates and streams instances of this class from status update records. The records are gathered and organised
     * for each job.
     *
     * @param  records the list of status update records
     * @return         a stream of instances of this class, each for a different job
     */
    public static Stream<JobStatusUpdates> streamFrom(Stream<JobStatusUpdateRecord> records) {
        JobStatusesBuilder builder = new JobStatusesBuilder();
        records.forEach(builder::update);
        return builder.stream();
    }

    /**
     * Creates and streams instances of this class from status update records, where all records for each job are
     * consecutive. Only the records for one job are held in memory at a time. Jobs are returned in the order they
     * occur in the input. If the records for a job are not consecutive, that job will be returned more than once, with
     * each instance holding only some of its records.
     *
     * @param  records the status update records, grouped by job
     * @return         a stream of instances of this class, each for a different job
     */
    public static Stream<JobStatusUpdates> streamFromRecordsGroupedByJob(Stream<JobStatusUpdateRecord> records) {
        Iterator<JobStatusUpdates> iterator = new GroupedByJobIterator(records.iterator());
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(records::close);
    }

    public String getJobId() {
        return jobId;
    }

    public JobStatusUpdateRecord getFirstRecord() {
        return recordsLatestFirst.get(recordsLatestFirst.size() - 1);
    }

    public JobStatusUpdateRecord getLastRecord() {
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

    public JobRuns getRuns() {
        return runs;
    }

    private static List<JobStatusUpdateRecord> orderLatestFirst(List<JobStatusUpdateRecord> records) {
        return records.stream()
                .sorted(Comparator.comparing(JobStatusUpdateRecord::getUpdateTime).reversed())
                .collect(Collectors.toList());
    }

    /**
     * Iterates through jobs by gathering consecutive status update records with the same job ID. Reads ahead by one
     * record to find the end of each job.
     */
    private static class GroupedByJobIterator implements Iterator<JobStatusUpdates> {
        private final Iterator<JobStatusUpdateRecord> records;
        private JobStatusUpdateRecord nextRecord;

        GroupedByJobIterator(Iterator<JobStatusUpdateRecord> records) {
            this.records = records;
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null || records.hasNext();
        }

        @Override
        public JobStatusUpdates next() {
            JobStatusUpdateRecord firstRecord = nextRecord != null ? nextRecord : records.next();
            nextRecord = null;
            String jobId = firstRecord.getJobId();
            List<JobStatusUpdateRecord> jobRecords = new ArrayList<>();
            jobRecords.add(firstRecord);
            while (records.hasNext()) {
                JobStatusUpdateRecord record = records.next();
                if (jobId.equals(record.getJobId())) {
                    jobRecords.add(record);
                } else {
                    nextRecord = record;
                    break;
                }
            }
            return from(jobId, jobRecords);
        }
    }
}
