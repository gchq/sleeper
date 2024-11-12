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

package sleeper.query.core.model;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryValidationException extends RuntimeException {
    private final String queryId;
    private final List<Map<String, String>> statusReportDestinations;

    public QueryValidationException(String queryId, List<Map<String, String>> statusReportDestinations, String message) {
        super(buildMessage(queryId, message));
        this.queryId = queryId;
        this.statusReportDestinations = statusReportDestinations;
    }

    public QueryValidationException(String queryId, List<Map<String, String>> statusReportDestinations, Exception cause) {
        super(buildMessage(queryId, cause.getMessage()), cause);
        this.queryId = queryId;
        this.statusReportDestinations = statusReportDestinations;
    }

    public Optional<String> getQueryId() {
        return Optional.ofNullable(queryId);
    }

    public List<Map<String, String>> getStatusReportDestinations() {
        return statusReportDestinations;
    }

    private static String buildMessage(String queryId, String message) {
        if (queryId == null) {
            return "Query validation failed: " + message;
        } else {
            return "Query validation failed for query \"" + queryId + "\": " + message;
        }
    }
}
