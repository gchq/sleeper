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
package sleeper.query.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;

import java.util.Map;
import java.util.Optional;

/**
 * Serialises a {@link Query} to and from JSON.
 */
public class QuerySerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final SchemaLoader schemaLoader;

    private QuerySerDe(SchemaLoader schemaLoader) {
        GsonBuilder builder = new GsonBuilder()
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.schemaLoader = schemaLoader;
    }

    public QuerySerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(tableName -> {
            try {
                return Optional.of(tablePropertiesProvider.getByName(tableName))
                        .map(TableProperties::getSchema);
            } catch (TablePropertiesProvider.TableNotFoundException e) {
                return Optional.empty();
            }
        });
    }

    public QuerySerDe(Map<String, Schema> tableNameToSchemaMap) {
        this(tableName -> Optional.ofNullable(tableNameToSchemaMap.get(tableName)));
    }

    public String toJson(Query query) {
        return toJson(query.toNew());
    }

    public String toJson(QueryNew query) {
        return gson.toJson(QueryJson.from(query, schemaLoader));
    }

    public String toJson(LeafPartitionQuery leafQuery) {
        return gson.toJson(QueryJson.from(leafQuery, schemaLoader));
    }

    public String toJson(Query query, boolean prettyPrint) {
        return toJson(query.toNew(), prettyPrint);
    }

    public String toJson(QueryNew query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(QueryJson.from(query, schemaLoader));
        }
        return toJson(query);
    }

    public String toJson(LeafPartitionQuery leafQuery, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(QueryJson.from(leafQuery, schemaLoader));
        }
        return toJson(leafQuery);
    }

    public Query fromJson(String json) {
        QueryJson queryJson = gson.fromJson(json, QueryJson.class);
        return queryJson.toParentQuery(schemaLoader).toOld();
    }

    public QueryOrLeafQuery fromJsonOrLeafQuery(String json) {
        QueryJson queryJson = gson.fromJson(json, QueryJson.class);
        return queryJson.toQueryOrLeafQuery(schemaLoader);
    }

    interface SchemaLoader {
        Optional<Schema> getSchema(String tableName) throws QueryValidationException;
    }
}
