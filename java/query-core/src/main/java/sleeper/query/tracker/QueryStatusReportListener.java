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

import java.util.List;
import java.util.Map;

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

    static QueryStatusReportListener fromConfig(Map<String, String> destinationConfig) {
        if (!destinationConfig.containsKey(QueryStatusReportListener.DESTINATION)) {
            throw new IllegalArgumentException(QueryStatusReportListener.class.getSimpleName() + " config: " + destinationConfig + " is missing attribute: " + QueryStatusReportListener.DESTINATION);
        }

        String destination = destinationConfig.get(QueryStatusReportListener.DESTINATION);
        if (destination.equals(WebSocketQueryStatusReportDestination.DESTINATION_NAME)) {
            return new WebSocketQueryStatusReportDestination(destinationConfig);
        } else if (destination.equals(DynamoDBQueryTracker.DESTINATION)) {
            return new DynamoDBQueryTracker(destinationConfig);
        }

        throw new IllegalArgumentException("Unrecognised " + QueryStatusReportListener.class.getSimpleName() + " " + QueryStatusReportListener.DESTINATION + ": " + destination);
    }
}
