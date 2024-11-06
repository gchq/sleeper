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
package sleeper.query.core.tracker;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;

import java.util.List;

public interface QueryStatusReportListener {
    String DESTINATION = "destination";

    void queryQueued(Query query);

    void queryInProgress(Query query);

    void queryInProgress(LeafPartitionQuery leafQuery);

    void subQueriesCreated(Query query, List<LeafPartitionQuery> subQueries);

    void queryCompleted(Query query, ResultsOutputInfo outputInfo);

    void queryCompleted(LeafPartitionQuery leafQuery, ResultsOutputInfo outputInfo);

    void queryFailed(Query query, Exception e);

    void queryFailed(String queryId, Exception e);

    void queryFailed(LeafPartitionQuery leafQuery, Exception e);

}
