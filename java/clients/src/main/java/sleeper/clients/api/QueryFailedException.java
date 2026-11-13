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
package sleeper.clients.api;

import sleeper.query.core.tracker.TrackedQuery;

/**
 * Thrown when a query did not complete successfully, based on its entry in the query tracker.
 */
public class QueryFailedException extends RuntimeException {

    private final transient TrackedQuery trackedQuery;

    public QueryFailedException(TrackedQuery trackedQuery) {
        super("Query " + trackedQuery.getQueryId() + " finished with state " + trackedQuery.getLastKnownState()
                + (trackedQuery.getErrorMessage() != null ? ": " + trackedQuery.getErrorMessage() : ""));
        this.trackedQuery = trackedQuery;
    }

    public TrackedQuery getTrackedQuery() {
        return trackedQuery;
    }
}
