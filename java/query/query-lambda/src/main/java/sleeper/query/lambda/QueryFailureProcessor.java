/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.lambda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.tracker.QueryStatusReportListener;
import sleeper.query.core.tracker.QueryTrackerException;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.core.tracker.TrackedQuery;

/**
 * Ensures a query is marked as failed in the query tracker. This is used to handle queries whose processing
 * lambda failed without reporting the failure itself (for example, when the lambda was killed by an execution
 * timeout or ran out of memory). Without this, such a query would remain in progress in the tracker indefinitely,
 * and its parent query would never resolve.
 */
public class QueryFailureProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryFailureProcessor.class);

    private final QueryTrackerStore trackerStore;
    private final QueryStatusReportListener trackerListener;

    public QueryFailureProcessor(QueryTrackerStore trackerStore, QueryStatusReportListener trackerListener) {
        this.trackerStore = trackerStore;
        this.trackerListener = trackerListener;
    }

    public void queryFailed(LeafPartitionQuery leafQuery) {
        if (isAlreadyFinished(leafQuery)) {
            LOGGER.info("Leaf query {} sub-query {} has already finished, not marking as failed",
                    leafQuery.getQueryId(), leafQuery.getSubQueryId());
            return;
        }

        LOGGER.info("Marking leaf query {} sub-query {} as failed", leafQuery.getQueryId(), leafQuery.getSubQueryId());
        trackerListener.queryFailed(leafQuery,
                new QueryProcessingFailedException("Query processing lambda failed to process this sub-query "
                        + "(it may have timed out or run out of memory)."));
    }

    private boolean isAlreadyFinished(LeafPartitionQuery leafQuery) {
        try {
            TrackedQuery tracked = trackerStore.getStatus(leafQuery.getQueryId(), leafQuery.getSubQueryId());
            return tracked != null && tracked.getLastKnownState().isFinished();
        } catch (QueryTrackerException e) {
            LOGGER.warn("Could not read the current state of leaf query {} sub-query {}",
                    leafQuery.getQueryId(), leafQuery.getSubQueryId(), e);
            return false;
        }
    }

    /**
     * Thrown to record why a leaf partition query is being marked as failed by this handler.
     */
    public static class QueryProcessingFailedException extends RuntimeException {
        public QueryProcessingFailedException(String message) {
            super(message);
        }
    }

}
