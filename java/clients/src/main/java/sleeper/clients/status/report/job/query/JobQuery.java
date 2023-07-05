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
package sleeper.clients.status.report.job.query;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Clock;
import java.util.List;

public interface JobQuery {

    List<CompactionJobStatus> run(CompactionJobStatusStore statusStore);

    List<IngestJobStatus> run(IngestJobStatusStore statusStore);

    static JobQuery from(String tableName, Type queryType, String queryParameters, Clock clock) {
        if (queryType.isParametersRequired() && queryParameters == null) {
            throw new IllegalArgumentException("No parameters provided for query type " + queryType);
        }
        switch (queryType) {
            case ALL:
                return new AllJobsQuery(tableName);
            case UNFINISHED:
                return new UnfinishedJobsQuery(tableName);
            case DETAILED:
                return DetailedJobsQuery.fromParameters(queryParameters);
            case RANGE:
                return RangeJobsQuery.fromParameters(tableName, queryParameters, clock);
            case REJECTED:
                return new RejectedJobsQuery();
            default:
                throw new IllegalArgumentException("Unexpected query type: " + queryType);
        }
    }

    static JobQuery fromParametersOrPrompt(
            String tableName, Type queryType, String queryParameters, Clock clock, ConsoleInput input) {
        if (queryType == JobQuery.Type.PROMPT) {
            return JobQueryPrompt.from(tableName, clock, input);
        }
        return from(tableName, queryType, queryParameters, clock);
    }

    enum Type {
        PROMPT,
        ALL,
        DETAILED,
        RANGE,
        UNFINISHED,
        REJECTED;

        public boolean isParametersRequired() {
            return this == DETAILED;
        }
    }
}
