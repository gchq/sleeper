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
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;
import sleeper.core.range.RegionSerDe.RegionJsonSerDe;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Serialises a {@link Query} to and from JSON.
 */
public class QuerySerDe {
    public static final String QUERY_ID = "queryId";
    public static final String SUB_QUERY_ID = "subQueryId";
    public static final String REQUESTED_VALUE_FIELDS = "requestedValueFields";
    public static final String TABLE_NAME = "tableName";
    public static final String QUERY_ITERATOR_CLASS_NAME = "queryTimeIteratorClassName";
    public static final String QUERY_ITERATOR_CONFIG = "queryTimeIteratorConfig";
    public static final String RESULTS_PUBLISHER_CONFIG = "resultsPublisherConfig";
    public static final String STATUS_REPORT_DESTINATIONS = "statusReportDestinations";
    public static final String QUERY_TYPE = "type";
    public static final String QUERY = "Query";
    public static final String LEAF_PARTITION_QUERY = "LeafPartitionQuery";
    public static final String FILES = "files";
    public static final String PARTITION_REGION = "partitionRegion";
    public static final String REGIONS = "regions";
    public static final String LEAF_PARTITION_ID = "leafPartitionId";

    private final Gson gson;
    private final Gson gsonPrettyPrinting;

    private QuerySerDe(Supplier<QueryJsonSerDe> queryJsonSerDeSupplier) {
        try {
            GsonBuilder builder = new GsonBuilder()
                    .registerTypeAdapter(Class.forName(Query.class.getName()), queryJsonSerDeSupplier.get())
                    .registerTypeAdapter(Class.forName(LeafPartitionQuery.class.getName()), queryJsonSerDeSupplier.get())
                    .serializeNulls();
            gson = builder.create();
            gsonPrettyPrinting = builder.setPrettyPrinting().create();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Exception creating Gson", e);
        }
    }

    public QuerySerDe(TablePropertiesProvider tablePropertiesProvider) {
        this(() -> new QueryJsonSerDe(tablePropertiesProvider));
    }

    public QuerySerDe(Map<String, Schema> tableNameToSchemaMap) {
        this(() -> new QueryJsonSerDe(tableNameToSchemaMap));
    }

    private static JsonArray convertRegionsToJsonArray(Schema schema, List<Region> regions,
                                                       java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
        RegionJsonSerDe regionJsonSerDe = new RegionJsonSerDe(schema);
        JsonArray array = new JsonArray();
        for (Region region : regions) {
            array.add(regionJsonSerDe.serialize(region, typeOfSrc, context));
        }
        return array;
    }

    private static JsonArray convertRegionsToJsonArray(
            Schema schema, List<Region> regions, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context,
            String tableName, String queryId) {
        try {
            return convertRegionsToJsonArray(schema, regions, typeOfSrc, context);
        } catch (RegionSerDe.KeyDoesNotExistException e) {
            throw new QueryValidationException(queryId,
                    "Key \"" + e.getKeyName() + "\" does not exist on table \"" + tableName + "\"");
        }
    }

    private static List<Region> convertJsonArrayToRegions(
            Schema schema, JsonArray array, java.lang.reflect.Type typeOfSrc, JsonDeserializationContext context) {
        RegionJsonSerDe regionJsonSerDe = new RegionJsonSerDe(schema);
        List<Region> regions = new ArrayList<>();
        Iterator<JsonElement> it = array.iterator();
        while (it.hasNext()) {
            regions.add(regionJsonSerDe.deserialize(it.next(), typeOfSrc, context));
        }
        return regions;
    }

    private static List<Region> convertJsonArrayToRegions(
            Schema schema, JsonArray array, java.lang.reflect.Type typeOfSrc, JsonDeserializationContext context,
            String tableName, String queryId) {
        try {
            return convertJsonArrayToRegions(schema, array, typeOfSrc, context);
        } catch (RegionSerDe.KeyDoesNotExistException e) {
            throw new QueryValidationException(queryId,
                    "Key \"" + e.getKeyName() + "\" does not exist on table \"" + tableName + "\"");
        }
    }

    public String toJson(Query query) {
        return gson.toJson(query);
    }

    public String toJson(Query query, boolean prettyPrint) {
        if (prettyPrint) {
            return gsonPrettyPrinting.toJson(query);
        }
        return toJson(query);
    }

    public Query fromJson(String jsonQuery) {
        return gson.fromJson(jsonQuery, Query.class);
    }

    public static class QueryJsonSerDe implements JsonSerializer<Query>, JsonDeserializer<Query> {
        private final SchemaLoader schemaLoader;

        public QueryJsonSerDe(SchemaLoader schemaLoader) {
            this.schemaLoader = schemaLoader;
        }

        public QueryJsonSerDe(TablePropertiesProvider tablePropertiesProvider) {
            this((queryId, tableName) -> {
                if (tableName == null) {
                    throw new QueryValidationException(queryId, "Table must not be null");
                }
                try {
                    return tablePropertiesProvider.getByName(tableName).getSchema();
                } catch (TablePropertiesProvider.TableNotFoundException e) {
                    throw new QueryValidationException(queryId, "Table \"" + tableName + "\" does not exist");
                }
            });
        }

        public QueryJsonSerDe(Map<String, Schema> tableNameToSchemaMap) {
            this((queryId, tableName) -> {
                if (tableName == null) {
                    throw new QueryValidationException(queryId, "Table must not be null");
                }
                if (!tableNameToSchemaMap.containsKey(tableName)) {
                    throw new QueryValidationException(queryId, "Table \"" + tableName + "\" does not exist");
                }
                return tableNameToSchemaMap.get(tableName);
            });
        }

        @Override
        public JsonElement serialize(Query query, java.lang.reflect.Type typeOfSrc, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            json.addProperty(QUERY_ID, query.getQueryId());

            if (query.getRequestedValueFields() != null) {
                JsonArray requestedValueFields = new JsonArray();
                query.getRequestedValueFields().forEach(requestedValueFields::add);
                json.add(REQUESTED_VALUE_FIELDS, requestedValueFields);
            }

            json.addProperty(TABLE_NAME, query.getTableName());
            if (null != query.getQueryTimeIteratorClassName()) {
                json.addProperty(QUERY_ITERATOR_CLASS_NAME, query.getQueryTimeIteratorClassName());
                if (null != query.getQueryTimeIteratorConfig()) {
                    json.addProperty(QUERY_ITERATOR_CONFIG, query.getQueryTimeIteratorConfig());
                }
            }

            JsonObject resultsPublisherConfig = new JsonObject();
            for (Map.Entry<String, String> entry : query.getResultsPublisherConfig().entrySet()) {
                resultsPublisherConfig.add(entry.getKey(), new JsonPrimitive(entry.getValue()));
            }
            json.add(RESULTS_PUBLISHER_CONFIG, resultsPublisherConfig);

            if (query.getStatusReportDestinations() != null) {
                JsonArray statusReportDestinations = new JsonArray();
                for (Map<String, String> statusReportDestination : query.getStatusReportDestinations()) {
                    JsonObject destination = new JsonObject();
                    for (Map.Entry<String, String> entry : statusReportDestination.entrySet()) {
                        destination.add(entry.getKey(), new JsonPrimitive(entry.getValue()));
                    }
                    statusReportDestinations.add(destination);
                }
                json.add(STATUS_REPORT_DESTINATIONS, statusReportDestinations);
            }

            Schema schema = schemaLoader.getTableForQuery(query.getQueryId(), query.getTableName());

            if (query instanceof LeafPartitionQuery) {
                json.addProperty(QUERY_TYPE, LEAF_PARTITION_QUERY);
                LeafPartitionQuery leafPartitionQuery = (LeafPartitionQuery) query;
                json.addProperty(SUB_QUERY_ID, leafPartitionQuery.getSubQueryId());

                JsonElement serialisedPartitionRegion = new RegionJsonSerDe(schema).serialize(leafPartitionQuery.getPartitionRegion(), typeOfSrc, context);
                json.add(PARTITION_REGION, serialisedPartitionRegion);

                List<String> files = leafPartitionQuery.getFiles();
                JsonArray fileArray = new JsonArray();
                for (String file : files) {
                    fileArray.add(file);
                }
                json.add(FILES, fileArray);
                json.add(REGIONS, convertRegionsToJsonArray(schema, leafPartitionQuery.getRegions(), typeOfSrc, context,
                        query.getTableName(), query.getQueryId()));

                json.addProperty(LEAF_PARTITION_ID, leafPartitionQuery.getLeafPartitionId());
            } else {
                json.addProperty(QUERY_TYPE, QUERY);
                json.add(REGIONS, convertRegionsToJsonArray(schema, query.getRegions(), typeOfSrc, context,
                        query.getTableName(), query.getQueryId()));
            }

            return json;
        }

        @Override
        public Query deserialize(JsonElement jsonElement, java.lang.reflect.Type typeOfSrc, JsonDeserializationContext context) throws JsonParseException {
            if (!jsonElement.isJsonObject()) {
                throw new JsonParseException("Expected JsonObject");
            }
            JsonObject jsonObject = (JsonObject) jsonElement;
            if (!jsonObject.has(QUERY_TYPE)) {
                throw new JsonParseException(QUERY_TYPE + " field must be provided");
            }
            String type = jsonObject.get(QUERY_TYPE).getAsString();
            if (!jsonObject.has(QUERY_ID)) {
                throw new JsonParseException(QUERY_ID + " field must be provided");
            }
            String queryId = jsonObject.get(QUERY_ID).getAsString();
            if (!jsonObject.has(TABLE_NAME) || JsonNull.INSTANCE.equals(jsonObject.get(TABLE_NAME))) {
                throw new JsonParseException(TABLE_NAME + " field must be provided");
            }
            String tableName = jsonObject.get(TABLE_NAME).getAsString();

            List<String> requestedValueFields = null;
            if (jsonObject.has(REQUESTED_VALUE_FIELDS)) {
                requestedValueFields = new ArrayList<>();
                JsonArray requestedValueFieldsJsonArray = jsonObject.getAsJsonArray(REQUESTED_VALUE_FIELDS);
                for (JsonElement element : requestedValueFieldsJsonArray) {
                    requestedValueFields.add(element.getAsString());
                }
            }

            String queryTimeIteratorClassName = null;
            String queryTimeIteratorConfig = null;
            if (null != jsonObject.get(QUERY_ITERATOR_CLASS_NAME)) {
                queryTimeIteratorClassName = jsonObject.get(QUERY_ITERATOR_CLASS_NAME).getAsString();
                if (null != jsonObject.get(QUERY_ITERATOR_CONFIG)) {
                    queryTimeIteratorConfig = jsonObject.get(QUERY_ITERATOR_CONFIG).getAsString();
                }
            }

            Map<String, String> resultsPublisherConfig = new HashMap<>();
            JsonObject resultsPublisherConfigJson = jsonObject.getAsJsonObject(RESULTS_PUBLISHER_CONFIG);
            if (null != resultsPublisherConfigJson) {
                for (Map.Entry<String, JsonElement> entry : resultsPublisherConfigJson.entrySet()) {
                    resultsPublisherConfig.put(entry.getKey(), entry.getValue().getAsString());
                }
            }

            List<Map<String, String>> statusReportDestinations = new ArrayList<>();
            JsonArray statusReportDestinationsJson = jsonObject.getAsJsonArray(STATUS_REPORT_DESTINATIONS);
            if (statusReportDestinationsJson != null) {
                for (JsonElement statusReportDestinationJsonElement : statusReportDestinationsJson) {
                    Map<String, String> statusReportDestination = new HashMap<>();
                    JsonObject statusReportDestinationJson = statusReportDestinationJsonElement.getAsJsonObject();
                    for (Map.Entry<String, JsonElement> entry : statusReportDestinationJson.entrySet()) {
                        statusReportDestination.put(entry.getKey(), entry.getValue().getAsString());
                    }
                    statusReportDestinations.add(statusReportDestination);
                }
            }

            Schema schema = schemaLoader.getTableForQuery(queryId, tableName);

            switch (type) {
                case LEAF_PARTITION_QUERY:
                    if (!jsonObject.has(PARTITION_REGION) || JsonNull.INSTANCE.equals(jsonObject.get(PARTITION_REGION))) {
                        throw new JsonParseException(PARTITION_REGION + " field must be provided");
                    }
                    Region partitionRegion = new RegionJsonSerDe(schema).deserialize(jsonObject.get(PARTITION_REGION), typeOfSrc, context);
                    JsonArray filesArray = jsonObject.getAsJsonArray(FILES);
                    String subQueryId = jsonObject.get(SUB_QUERY_ID).getAsString();
                    List<String> files = new ArrayList<>();
                    for (int i = 0; i < filesArray.size(); i++) {
                        files.add(filesArray.get(i).getAsString());
                    }
                    List<Region> regions = convertJsonArrayToRegions(schema, jsonObject.getAsJsonArray(REGIONS),
                            typeOfSrc, context, tableName, queryId);
                    String leafPartitionId = jsonObject.get(LEAF_PARTITION_ID).getAsString();

                    return new LeafPartitionQuery.Builder(tableName, queryId, subQueryId, regions, leafPartitionId, partitionRegion, files)
                            .setQueryTimeIteratorClassName(queryTimeIteratorClassName)
                            .setQueryTimeIteratorConfig(queryTimeIteratorConfig)
                            .setResultsPublisherConfig(resultsPublisherConfig)
                            .setStatusReportDestinations(statusReportDestinations)
                            .setRequestedValueFields(requestedValueFields)
                            .build();
                case QUERY:
                    List<Region> ranges = new ArrayList<>();
                    if (jsonObject.has(REGIONS)) {
                        ranges.addAll(convertJsonArrayToRegions(schema, jsonObject.getAsJsonArray(REGIONS),
                                typeOfSrc, context, tableName, queryId));
                    }
                    return new Query.Builder(tableName, queryId, ranges)
                            .setQueryTimeIteratorClassName(queryTimeIteratorClassName)
                            .setQueryTimeIteratorConfig(queryTimeIteratorConfig)
                            .setResultsPublisherConfig(resultsPublisherConfig)
                            .setStatusReportDestinations(statusReportDestinations)
                            .setRequestedValueFields(requestedValueFields)
                            .build();
                default:
                    throw new IllegalArgumentException("Unknown query type: " + type);
            }
        }
    }

    interface SchemaLoader {
        Schema getTableForQuery(String tableName, String queryId) throws QueryValidationException;
    }
}
