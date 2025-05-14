/*
 * Copyright 2022-2025 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public enum QueryState {
    COMPLETED,
    FAILED,
    QUEUED,
    IN_PROGRESS,
    PARTIALLY_FAILED;

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryState.class);

    public static Optional<QueryState> getParentStateIfFinished(String queryId, List<TrackedQuery> children) {
        boolean allCompleted = true;
        boolean allSucceeded = true;
        boolean allFailed = true;
        long activeCount = 0;
        for (TrackedQuery child : children) {
            switch (child.getLastKnownState()) {
                case FAILED:
                case PARTIALLY_FAILED:
                    allSucceeded = false;
                    break;
                case COMPLETED:
                    allFailed = false;
                    break;
                default:
                    activeCount++;
                    allCompleted = false;
            }
        }

        if (allCompleted && allSucceeded) {
            return Optional.of(COMPLETED);
        } else if (allCompleted && allFailed) {
            return Optional.of(FAILED);
        } else if (allCompleted) {
            return Optional.of(PARTIALLY_FAILED);
        } else {
            LOGGER.info("Found {} leaf queries are still either in progress or queued for query {}", activeCount, queryId);
            return Optional.empty();
        }
    }
}
