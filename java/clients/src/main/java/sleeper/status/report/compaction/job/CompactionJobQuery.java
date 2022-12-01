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

package sleeper.status.report.compaction.job;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.status.report.query.AllJobsQuery;
import sleeper.status.report.query.DetailedJobsQuery;
import sleeper.status.report.query.RangeJobsQuery;
import sleeper.status.report.query.UnfinishedJobsQuery;

import java.time.Clock;
import java.util.List;

import static sleeper.status.report.query.JobQuery.Type;

public interface CompactionJobQuery {

    static CompactionJobQuery from(String tableName, Type queryType, String queryParameters, Clock clock) {
        switch (queryType) {
            case ALL:
                return new AllJobsQuery(tableName).forCompaction();
            case UNFINISHED:
                return new UnfinishedJobsQuery(tableName).forCompaction();
            case DETAILED:
                return DetailedJobsQuery.fromParameters(queryParameters);
            case RANGE:
                return RangeJobsQuery.fromParameters(tableName, queryParameters, clock);
            default:
                throw new IllegalArgumentException("Unexpected query type: " + queryType);
        }
    }

    List<CompactionJobStatus> run(CompactionJobStatusStore statusStore);
}
