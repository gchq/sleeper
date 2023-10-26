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

package sleeper.query.tracker;

import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.output.ResultsOutputInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryQueryStatusReportListener implements QueryStatusReportListener {
    private final Map<String, Query> queriesById = new HashMap<>();
    private final Map<String, Exception> errorsByQueryId = new HashMap<>();
    private final Map<String, QueryState> stateByQueryId = new HashMap<>();

    @Override
    public void queryQueued(Query query) {
        stateByQueryId.put(query.getQueryId(), QueryState.QUEUED);
        queriesById.put(query.getQueryId(), query);
    }

    @Override
    public void queryInProgress(Query query) {
        stateByQueryId.put(query.getQueryId(), QueryState.IN_PROGRESS);
        queriesById.put(query.getQueryId(), query);
    }

    @Override
    public void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries) {
        subQueries.forEach(this::queryQueued);
    }

    @Override
    public void queryCompleted(Query query, ResultsOutputInfo outputInfo) {
        stateByQueryId.put(query.getQueryId(), QueryState.COMPLETED);
        queriesById.put(query.getQueryId(), query);
    }

    @Override
    public void queryFailed(Query query, Exception e) {
        stateByQueryId.put(query.getQueryId(), QueryState.FAILED);
        errorsByQueryId.put(query.getQueryId(), e);
        queriesById.put(query.getQueryId(), query);
    }
}
