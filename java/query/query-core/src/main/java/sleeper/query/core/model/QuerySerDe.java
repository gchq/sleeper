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
package sleeper.query.core.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableNotFoundException;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Serialises a query to and from JSON.
 */
public class QuerySerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final SchemaLoader schemaLoader;

    public QuerySerDe(SchemaLoader schemaLoader) {
        GsonBuilder builder = new GsonBuilder()
                .serializeNulls();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.schemaLoader = schemaLoader;
    }

    public QuerySerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(new SchemaLoaderFromTableProvider(tablePropertiesProvider));
    }

    public QuerySerDe(Schema schema) {
        this(new FixedSchemaLoader(schema));
    }

    /**
     * Convert a Sleeper query to JSON.
     *
     * @param  query the Sleeper query
     * @return       a JSON string
     */
    public String toJson(Query query) {
        return gson.toJson(QueryJson.from(query, schemaLoader));
    }

    /**
     * Convert a Sleeper leaf partition query to JSON.
     *
     * @param  leafQuery the Sleeper leaf partition query
     * @return           a JSON string
     */
    public String toJson(LeafPartitionQuery leafQuery) {
        return gson.toJson(QueryJson.from(leafQuery, schemaLoader));
    }

    /**
     * Convert a Sleeper query to JSON.
     *
     * @param  query       the Sleeper query
     * @param  prettyPrint set to true if the JSON should be formatted
     * @return             a formatted JSON string
     */
    public String toJson(Query query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(QueryJson.from(query, schemaLoader));
        }
        return toJson(query);
    }

    /**
     * Convert a Sleeper leaf partition query to JSON.
     *
     * @param  leafQuery   the Sleeper leaf partition query
     * @param  prettyPrint set to true if the JSON should be formatted
     * @return             a formatted JSON string
     */
    public String toJson(LeafPartitionQuery leafQuery, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(QueryJson.from(leafQuery, schemaLoader));
        }
        return toJson(leafQuery);
    }

    /**
     * Convert a json string into a Sleeper query.
     *
     * @param  json the JSON to convert
     * @return      a Sleeper query
     */
    public Query fromJson(String json) {
        QueryJson queryJson = gson.fromJson(json, QueryJson.class);
        return queryJson.toParentQuery(schemaLoader);
    }

    /**
     * Convert a JSON string into a Sleeper query or leaf partition query.
     *
     * @param  json the JSON to convert
     * @return      a Sleeper query or leaf partition query
     */
    public QueryOrLeafPartitionQuery fromJsonOrLeafQuery(String json) {
        QueryJson queryJson = gson.fromJson(json, QueryJson.class);
        return queryJson.toQueryOrLeafQuery(schemaLoader);
    }

    /**
     * Schema loader.
     */
    public interface SchemaLoader {
        /**
         * Return a schema from a given table name.
         *
         * @param  tableName the Sleeper table name
         * @return           a Sleeper table schema
         */
        Optional<Schema> getSchemaByTableName(String tableName);

        /**
         * Return a schema from a given table Id.
         *
         * @param  tableId the sleeper table Id
         * @return         a Sleeper table schema
         */
        Optional<Schema> getSchemaByTableId(String tableId);
    }

    /**
     * Retrieves Sleeper table schemas from a table provider.
     */
    private static class SchemaLoaderFromTableProvider implements SchemaLoader {

        private final TablePropertiesProvider provider;

        SchemaLoaderFromTableProvider(TablePropertiesProvider provider) {
            this.provider = provider;
        }

        @Override
        public Optional<Schema> getSchemaByTableName(String tableName) {
            return getSchema(() -> provider.getByName(tableName));
        }

        @Override
        public Optional<Schema> getSchemaByTableId(String tableId) {
            return getSchema(() -> provider.getById(tableId));
        }

        private Optional<Schema> getSchema(Supplier<TableProperties> getProperties) {
            try {
                return Optional.of(getProperties.get())
                        .map(TableProperties::getSchema);
            } catch (TableNotFoundException e) {
                return Optional.empty();
            }
        }
    }

    /**
     * Retrieves the Sleeper table schema.
     */
    private static class FixedSchemaLoader implements SchemaLoader {
        private final Schema schema;

        FixedSchemaLoader(Schema schema) {
            this.schema = schema;
        }

        @Override
        public Optional<Schema> getSchemaByTableName(String tableName) {
            return Optional.of(schema);
        }

        @Override
        public Optional<Schema> getSchemaByTableId(String tableId) {
            return Optional.of(schema);
        }
    }
}
