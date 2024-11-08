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

package sleeper.query.runner.tracker;

import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;

import java.time.Instant;

public class TrackedQueryTestHelper {
    private TrackedQueryTestHelper() {
    }

    public static TrackedQuery queryQueued(String queryId, Instant lastUpdateTime) {
        return queryWithState(queryId, lastUpdateTime, QueryState.QUEUED).build();
    }

    public static TrackedQuery queryInProgress(String queryId, Instant lastUpdateTime) {
        return queryWithState(queryId, lastUpdateTime, QueryState.IN_PROGRESS).build();
    }

    public static TrackedQuery queryCompleted(String queryId, Instant lastUpdateTime, long records) {
        return queryWithState(queryId, lastUpdateTime, QueryState.COMPLETED)
                .recordCount(records)
                .build();
    }

    public static TrackedQuery queryPartiallyFailed(String queryId, Instant lastUpdateTime, long records) {
        return queryPartiallyFailed(queryId, lastUpdateTime, records, "");
    }

    public static TrackedQuery queryPartiallyFailed(String queryId, Instant lastUpdateTime, long records, String errorMessage) {
        return queryWithState(queryId, lastUpdateTime, QueryState.PARTIALLY_FAILED)
                .recordCount(records)
                .errorMessage(errorMessage)
                .build();
    }

    public static TrackedQuery queryFailed(String queryId, Instant lastUpdateTime) {
        return queryFailed(queryId, lastUpdateTime, "");
    }

    public static TrackedQuery queryFailed(String queryId, Instant lastUpdateTime, String errorMessage) {
        return queryWithState(queryId, lastUpdateTime, QueryState.FAILED)
                .errorMessage(errorMessage).build();
    }

    public static TrackedQuery subQueryInProgress(String queryId, String subQueryId, Instant lastUpdateTime) {
        return queryWithState(queryId, lastUpdateTime, QueryState.IN_PROGRESS)
                .subQueryId(subQueryId)
                .build();
    }

    private static TrackedQuery.Builder queryWithState(String queryId, Instant lastUpdateTime, QueryState state) {
        return TrackedQuery.builder()
                .queryId(queryId)
                .lastKnownState(state)
                .lastUpdateTime(lastUpdateTime);
    }
}
