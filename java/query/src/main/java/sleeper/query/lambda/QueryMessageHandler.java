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

package sleeper.query.lambda;

import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.query.model.Query;
import sleeper.query.model.QueryOrLeafQuery;
import sleeper.query.model.QuerySerDe;
import sleeper.query.model.QueryValidationException;
import sleeper.query.tracker.QueryStatusReportListener;
import sleeper.query.tracker.QueryStatusReportListeners;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

public class QueryMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryMessageHandler.class);
    private final QueryStatusReportListener queryTracker;
    private final QuerySerDe querySerDe;
    private final Supplier<String> invalidQueryIdSupplier;

    public QueryMessageHandler(TablePropertiesProvider tablePropertiesProvider,
                               QueryStatusReportListener queryTracker) {
        this(tablePropertiesProvider, queryTracker, () -> UUID.randomUUID().toString());
    }

    public QueryMessageHandler(TablePropertiesProvider tablePropertiesProvider,
                               QueryStatusReportListener queryTracker,
                               Supplier<String> invalidQueryIdSupplier) {
        this.queryTracker = queryTracker;
        this.invalidQueryIdSupplier = invalidQueryIdSupplier;
        this.querySerDe = new QuerySerDe(tablePropertiesProvider);
    }

    public Optional<QueryOrLeafQuery> deserialiseAndValidate(String message) {
        try {
            QueryOrLeafQuery query = querySerDe.fromJsonOrSubQuery(message);
            LOGGER.info("Deserialised message to query {}", query);
            return Optional.of(query);
        } catch (JsonParseException e) {
            LOGGER.error("JSONParseException deserialising query from JSON {}", message, e);
            queryTracker.queryFailed(invalidQuery(), e);
            return Optional.empty();
        } catch (QueryValidationException e) {
            LOGGER.error("QueryValidationException validating query from JSON {}", message, e);
            QueryStatusReportListeners queryTrackers = QueryStatusReportListeners.fromConfig(e.getStatusReportDestinations());
            queryTrackers.add(queryTracker);
            queryTrackers.queryFailed(invalidQuery(e.getQueryId()), e);
            return Optional.empty();
        }
    }

    private Query invalidQuery() {
        return invalidQuery(invalidQueryIdSupplier.get());
    }

    private Query invalidQuery(String queryId) {
        return new Query.Builder(null, queryId, List.of()).build();
    }
}
