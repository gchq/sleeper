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

package sleeper.ingest.job.status;

import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdate;

import java.util.stream.Stream;

/**
 * Defines the states an ingest job can be in.
 */
public enum IngestJobStatusType {
    REJECTED(IngestJobRejectedStatus.class, 1),
    ACCEPTED(IngestJobAcceptedStatus.class, 2),
    FAILED(ProcessFailedStatus.class, 3),
    IN_PROGRESS(IngestJobStartedStatus.class, 4),
    FINISHED(ProcessFinishedStatus.class, 5);

    private final Class<?> statusUpdateClass;
    private final int order;

    IngestJobStatusType(Class<?> statusUpdateClass, int order) {
        this.statusUpdateClass = statusUpdateClass;
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    public boolean isRunInProgress() {
        return this == ACCEPTED || this == IN_PROGRESS;
    }

    public boolean isEndOfJob() {
        return this == REJECTED || this == FINISHED;
    }

    /**
     * Gets the status type for the provided process status update.
     *
     * @param  update the process status update
     * @return        the ingest job status type of the update
     */
    public static IngestJobStatusType of(ProcessStatusUpdate update) {
        return Stream.of(values())
                .filter(type -> type.statusUpdateClass.isInstance(update))
                .findFirst().orElseThrow();
    }

}
