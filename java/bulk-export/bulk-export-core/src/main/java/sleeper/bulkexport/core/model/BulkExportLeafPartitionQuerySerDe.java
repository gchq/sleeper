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
package sleeper.bulkexport.core.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableNotFoundException;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Serialises an leaf partition query for bulk export to and from JSON.
 */
public class BulkExportLeafPartitionQuerySerDe {
    private final Gson gson;
    private final Gson gsonPrettyPrinting;
    private final SchemaLoader schemaLoader;

    private BulkExportLeafPartitionQuerySerDe(SchemaLoader schemaLoader) {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
        gsonPrettyPrinting = builder.setPrettyPrinting().create();
        this.schemaLoader = schemaLoader;
    }

    public BulkExportLeafPartitionQuerySerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(new SchemaLoaderFromTableProvider(tablePropertiesProvider));
    }

    public BulkExportLeafPartitionQuerySerDe(Schema schema) {
        this(new FixedSchemaLoader(schema));
    }

    /**
     * Formats a query as a JSON string.
     *
     * @param  query the query to format
     * @return       a JSON string of the query
     */
    public String toJson(BulkExportLeafPartitionQuery query) {
        return gson.toJson(BulkExportLeafPartitionQueryJson.from(query, schemaLoader));
    }

    /**
     * Formats a query as a JSON string with the option to pretty print.
     *
     * @param  query       the query to format
     * @param  prettyPrint option to pretty print
     * @return             a JSON string of the query
     */
    public String toJson(BulkExportLeafPartitionQuery query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(BulkExportLeafPartitionQueryJson.from(query, schemaLoader));
        }
        return toJson(query);
    }

    /**
     * Parses a JSON string as a query.
     *
     * @param  json the JSON string to parse
     * @return      the parsed query
     */
    public BulkExportLeafPartitionQuery fromJson(String json) {
        BulkExportLeafPartitionQueryJson query = gson.fromJson(json, BulkExportLeafPartitionQueryJson.class);
        return query.to(schemaLoader);
    }

    /**
     * Loads the schema of a Sleeper table.
     */
    public interface SchemaLoader {
        /**
         * Get a schema by using the table id.
         *
         * @param  tableId the id of the table to get the schema for
         * @return         the schema
         */
        Optional<Schema> getSchemaByTableId(String tableId);
    }

    /**
     * Loads the schema of a Sleeper table from a properties provider.
     */
    private static class SchemaLoaderFromTableProvider implements SchemaLoader {

        private final TablePropertiesProvider provider;

        SchemaLoaderFromTableProvider(TablePropertiesProvider provider) {
            this.provider = provider;
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
     * Loads a fixed Sleeper table schema.
     */
    private static class FixedSchemaLoader implements SchemaLoader {
        private final Schema schema;

        FixedSchemaLoader(Schema schema) {
            this.schema = schema;
        }

        @Override
        public Optional<Schema> getSchemaByTableId(String tableId) {
            return Optional.of(schema);
        }
    }
}
