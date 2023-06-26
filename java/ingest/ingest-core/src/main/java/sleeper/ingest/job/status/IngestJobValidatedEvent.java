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

package sleeper.ingest.job.status;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class IngestJobValidatedEvent {
    private final IngestJob job;
    private final String jobRunId;
    private final Instant validationTime;
    private final List<String> reasons;

    private IngestJobValidatedEvent(IngestJob job, String jobRunId, Instant validationTime, List<String> reasons) {
        this.job = job;
        this.jobRunId = jobRunId;
        this.validationTime = validationTime;
        this.reasons = reasons;
    }

    public static IngestJobValidatedEvent ingestJobAccepted(IngestJob job, String jobRunId, Instant validationTime) {
        return new IngestJobValidatedEvent(job, jobRunId, validationTime, List.of());
    }

    public static IngestJobValidatedEvent ingestJobRejected(IngestJob job, Instant validationTime, String... reasons) {
        return new IngestJobValidatedEvent(job, null, validationTime, List.of(reasons));
    }

    public static IngestJobValidatedEvent ingestJobRejected(IngestJob job, Instant validationTime, List<String> reasons) {
        return new IngestJobValidatedEvent(job, null, validationTime, reasons);
    }

    public IngestJob getJob() {
        return job;
    }

    public String getJobRunId() {
        return jobRunId;
    }

    public Instant getValidationTime() {
        return validationTime;
    }

    public boolean isAccepted() {
        return reasons.isEmpty();
    }

    public List<String> getReasons() {
        return reasons;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobValidatedEvent that = (IngestJobValidatedEvent) o;
        return Objects.equals(jobRunId, that.jobRunId)
                && Objects.equals(job, that.job)
                && Objects.equals(validationTime, that.validationTime)
                && Objects.equals(reasons, that.reasons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobRunId, job, validationTime, reasons);
    }

    @Override
    public String toString() {
        return "IngestJobValidatedEvent{" +
                "jobRunId='" + jobRunId + '\'' +
                ", job=" + job +
                ", validationTime=" + validationTime +
                ", reasons='" + reasons + '\'' +
                '}';
    }
}
