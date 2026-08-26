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
package sleeper.query.core.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableNotFoundException;

import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Serialises a query to and from JSON.
 * <p>
 * This class provides methods to convert Sleeper query objects, including
 * query and leaf partition query, into JSON
 * strings and to reconstruct these objects from JSON. It handles both
 * compact and pretty-printed JSON formats.
 * <p>
 * The serialisation process relies on a {@link SchemaLoader} to retrieve
 * necessary schema information for the queries, ensuring proper handling
 * of data structures.
 *
 */
public class QuerySerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final SchemaLoader schemaLoader;

    public QuerySerDe(SchemaLoader schemaLoader) {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.schemaLoader = schemaLoader;
    }

    public QuerySerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(new SchemaLoaderFromTableProvider(tablePropertiesProvider));
    }

    public QuerySerDe(String tableId, String tableName, Schema schema) {
        this(new FixedSchemaLoader(tableId, tableName, schema));
    }

    /**
     * Converts a Sleeper Query object to a compact JSON string.
     * This method uses the internal {@code Gson} instance configured for
     * compact output.
     *
     * @param  query the Sleeper query
     * @return       a JSON string
     */
    public String toJson(Query query) {
        return gson.toJson(QueryJson.from(query, schemaLoader));
    }

    /**
     * Converts a Sleeper LeafPartitionQuery object to a compact JSON
     * string.
     * This method uses the internal {@code Gson} instance configured for
     * compact output.
     *
     * @param  leafQuery the Sleeper leaf partition query
     * @return           a JSON string
     */
    public String toJson(LeafPartitionQuery leafQuery) {
        return gson.toJson(QueryJson.from(leafQuery, schemaLoader));
    }

    /**
     * Converts a Sleeper Query object to a JSON string, with pretty-printing.
     * The JSON output will be formatted for readability.
     *
     * @param  query the Sleeper query
     * @return       a formatted JSON string
     */
    public String toJsonPrettyPrint(Query query) {
        return gsonPrettyPrinting.toJson(QueryJson.from(query, schemaLoader));
    }

    /**
     * Converts a Sleeper LeafPartitionQuery object to a JSON string, with pretty-printing.
     * The JSON output will be formatted for readability.
     *
     * @param  leafQuery the Sleeper leaf partition query
     * @return           a formatted JSON string
     */
    public String toJsonPrettyPrint(LeafPartitionQuery leafQuery) {
        return gsonPrettyPrinting.toJson(QueryJson.from(leafQuery, schemaLoader));
    }

    /**
     * Converts a JSON string into a Sleeper Query object.
     * This method deserialises the JSON string into an intermediate
     * {@code QueryJson} object before converting it to the final
     * {@link Query} representation using the provided schema loader.
     *
     * @param  json the JSON to convert
     * @return      a Sleeper query
     */
    public Query fromJson(String json) {
        QueryJson queryJson = gson.fromJson(json, QueryJson.class);
        return queryJson.toParentQuery(schemaLoader);
    }

    /**
     * Converts a JSON string into either a Sleeper Query or a
     * LeafPartitionQuery object.
     * This method is flexible, attempting to deserialise the JSON into
     * whichever type is appropriate based on the content of the JSON and
     * then converting it using the schema loader.
     *
     * @param  json the JSON to convert
     * @return      a Sleeper query or leaf partition query
     */
    public QueryOrLeafPartitionQuery fromJsonOrLeafQuery(String json) {
        QueryJson queryJson = gson.fromJson(json, QueryJson.class);
        return queryJson.toQueryOrLeafQuery(schemaLoader);
    }

    /**
     * Schema loader. This is required to serialise/deserialise regions within
     * RegionSerDe.
     */
    public interface SchemaLoader {
        /**
         * Retrieves the schema for a Sleeper table by its name.
         *
         * @param  tableName the Sleeper table name
         * @return           a Sleeper table schema
         */
        Optional<Schema> getSchemaByTableName(String tableName);

        /**
         * Retrieves the schema for a Sleeper table by its ID.
         *
         * @param  tableId the sleeper table Id
         * @return         a Sleeper table schema
         */
        Optional<Schema> getSchemaByTableId(String tableId);

        /**
         * Resolves the unique ID of a Sleeper table from its name.
         *
         * @param  tableName the Sleeper table name
         * @return           the unique ID of the table
         */
        String getTableIdForName(String tableName);

        /**
         * Resolves the name of a Sleeper table from its unique ID.
         *
         * @param  tableId the unique ID of the Sleeper table
         * @return         the name of the table
         */
        String getTableNameForId(String tableId);
    }

    /**
     * An implementation of SchemaLoader that retrieves Sleeper table
     * schemas using a TablePropertiesProvider.
     * This class handles cases where a table may not be found by returning
     * an empty {@link java.util.Optional Optional}.
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

        @Override
        public String getTableIdForName(String tableName) {
            return provider.getByName(tableName).get(TABLE_ID);
        }

        @Override
        public String getTableNameForId(String tableId) {
            return provider.getById(tableId).get(TABLE_NAME);
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
     * An implementation of SchemaLoader that always returns a fixed,
     * predefined schema, regardless of the table name or ID requested.
     * This is useful for scenarios where a dynamic schema lookup is not
     * required or desirable.
     */
    private static class FixedSchemaLoader implements SchemaLoader {
        private final String tableId;
        private final String tableName;
        private final Schema schema;

        FixedSchemaLoader(String tableId, String tableName, Schema schema) {
            this.tableId = tableId;
            this.tableName = tableName;
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

        @Override
        public String getTableIdForName(String tableName) {
            return tableId;
        }

        @Override
        public String getTableNameForId(String tableId) {
            return tableName;
        }
    }
}
