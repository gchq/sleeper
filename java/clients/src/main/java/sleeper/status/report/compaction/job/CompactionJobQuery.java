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

import java.util.Collections;
import java.util.List;

import static sleeper.status.report.compaction.job.CompactionJobStatusReporter.QueryType;

public class CompactionJobQuery {
    private final String tableName;
    private final QueryType queryType;

    private CompactionJobQuery(String tableName, QueryType queryType) {
        this.tableName = tableName;
        this.queryType = queryType;
    }

    public static CompactionJobQuery from(String tableName, QueryType queryType) {
        return new CompactionJobQuery(tableName, queryType);
    }

    public List<CompactionJobStatus> run(CompactionJobStatusStore statusStore) {
        if (queryType.equals(QueryType.ALL)) {
            return statusStore.getAllJobs(tableName);
        }
        return Collections.emptyList();
    }
}
