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
import sleeper.query.model.QuerySerDe;
import sleeper.query.tracker.DynamoDBQueryTracker;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class QueryValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryValidator.class);
    private final DynamoDBQueryTracker queryTracker;
    private final QuerySerDe querySerDe;
    private final Supplier<String> invalidQueryIdSupplier;

    public QueryValidator(TablePropertiesProvider tablePropertiesProvider,
                          DynamoDBQueryTracker queryTracker,
                          Supplier<String> invalidQueryIdSupplier) {
        this.queryTracker = queryTracker;
        this.invalidQueryIdSupplier = invalidQueryIdSupplier;
        this.querySerDe = new QuerySerDe(tablePropertiesProvider);
    }

    public Optional<Query> deserialiseAndValidate(String message) {
        Query query;
        try {
            query = querySerDe.fromJson(message);
            LOGGER.info("Deserialised message to query {}", query);
        } catch (JsonParseException e) {
            LOGGER.error("JSONParseException deserialsing query from JSON {}", message);
            queryTracker.queryFailed(invalidQuery(), e);
            return Optional.empty();
        }
        return Optional.empty();
    }

    private Query invalidQuery() {
        return new Query.Builder(null, invalidQueryIdSupplier.get(), List.of()).build();
    }
}
