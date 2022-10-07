/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.job.status;

import java.time.Instant;
import java.util.Objects;

public class CompactionJobStatusUpdateRecord {

    private final String jobId;
    private final Instant expiryDate;
    private final CompactionJobStatusUpdate update;

    public CompactionJobStatusUpdateRecord(String jobId, Instant expiryDate, CompactionJobStatusUpdate update) {
        this.jobId = Objects.requireNonNull(jobId, "jobId must not be null");
        this.expiryDate = Objects.requireNonNull(expiryDate, "expiryDate must not be null");
        this.update = Objects.requireNonNull(update, "update must not be null");
    }

    public void addToBuilder(CompactionJobStatusesBuilder builder) {
        update.addToBuilder(builder, jobId);
        builder.expiryDate(jobId, expiryDate);
    }
}
