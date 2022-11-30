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
import sleeper.status.report.compaction.job.query.AllCompactionJobQuery;
import sleeper.status.report.compaction.job.query.DetailedCompactionJobQuery;
import sleeper.status.report.compaction.job.query.RangeCompactionJobQuery;
import sleeper.status.report.compaction.job.query.UnfinishedCompactionJobQuery;

import java.util.List;

import static sleeper.status.report.compaction.job.CompactionJobStatusReporter.QueryType;

public interface CompactionJobQuery {

    static CompactionJobQuery from(String tableName, QueryType queryType, String queryParameters) {
        switch (queryType) {
            case ALL:
                return new AllCompactionJobQuery(tableName);
            case UNFINISHED:
                return new UnfinishedCompactionJobQuery(tableName);
            case DETAILED:
                return DetailedCompactionJobQuery.fromParameters(queryParameters);
            case RANGE:
                return RangeCompactionJobQuery.fromParameters(tableName, queryParameters);
            default:
                throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    List<CompactionJobStatus> run(CompactionJobStatusStore statusStore);
}
