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
package sleeper.api.resources;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.api.AWSArchitectureResources.Resource;
import sleeper.api.AWSArchitectureResources.ResourcesResponse;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.table.TableStatus;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutput;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.core.tracker.QueryState;
import sleeper.query.core.tracker.TrackedQuery;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static sleeper.api.AWSArchitectureResources.dynamoTable;
import static sleeper.api.AWSArchitectureResources.lambdaFunction;
import static sleeper.api.AWSArchitectureResources.s3Bucket;
import static sleeper.api.AWSArchitectureResources.sqsQueue;
import static sleeper.api.ResourceUtils.loadPropertiesAndCheckStackEnabled;
import static sleeper.api.ResourceUtils.notAvailable;
import static sleeper.api.ResourceUtils.tableNamesById;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_FAILURE_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_FAILURE_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.core.properties.instance.QueryProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

@Path("/api")
public class QueryResource {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 1000;
    private static final int DEFAULT_RESULTS_LIMIT = 100;
    private static final int MAX_RESULTS_LIMIT = 5000;
    private static final String NON_NESTED_QUERY_PLACEHOLDER = "-";

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;
    private final CloudWatchClient cloudWatchClient;
    private final LambdaClient lambdaClient;
    private final String instanceId;
    private final String accountName;

    @Inject
    public QueryResource(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            SqsClient sqsClient,
            CloudWatchClient cloudWatchClient,
            LambdaClient lambdaClient,
            @ConfigProperty(name = "sleeper.instance.id") String instanceId,
            @ConfigProperty(name = "sleeper.account.name") String accountName) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.cloudWatchClient = cloudWatchClient;
        this.lambdaClient = lambdaClient;
        this.sqsClient = sqsClient;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Path("/queries")
    @Produces(MediaType.APPLICATION_JSON)
    public QueriesResponse getQueries(
            @QueryParam("tableId") String tableId,
            @QueryParam("limit") Integer limitParam,
            @QueryParam("queryId") String queryIdFilter,
            @QueryParam("state") String stateFilter,
            @QueryParam("from") Long fromEpochMillis,
            @QueryParam("to") Long toEpochMillis) {
        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Map<String, String> tableNamesById = tableNamesById(dynamoDbClient, instanceProperties);

        int limit = limitParam == null ? DEFAULT_LIMIT : Math.min(Math.max(1, limitParam), MAX_LIMIT);
        StateFilter state = StateFilter.parse(stateFilter);

        List<TrackedQuery> summaries = new ArrayList<>();
        for (TrackedQuery query : tracker.getAllQueries()) {
            // Only show top-level queries in the list, not their leaf sub-queries.
            if (!NON_NESTED_QUERY_PLACEHOLDER.equals(query.getSubQueryId())) {
                continue;
            }
            if (tableId != null && !tableId.isBlank() && !tableId.equals(query.getTableId())) {
                continue;
            }
            if (queryIdFilter != null && !queryIdFilter.isBlank() && !queryIdFilter.trim().equals(query.getQueryId())) {
                continue;
            }
            if (!state.matches(query)) {
                continue;
            }
            Long updated = toMillis(query.getLastUpdateTime());
            if (fromEpochMillis != null && (updated == null || updated < fromEpochMillis)) {
                continue;
            }
            if (toEpochMillis != null && (updated == null || updated > toEpochMillis)) {
                continue;
            }
            summaries.add(query);
        }

        // Newest first, by last update time (queries with no update time sort last).
        summaries.sort(Comparator.comparingLong(
                (TrackedQuery query) -> query.getLastUpdateTime() == null ? Long.MIN_VALUE : query.getLastUpdateTime())
                .reversed());

        int total = summaries.size();
        int numToReturn = Math.min(limit, total);
        List<QuerySummary> page = new ArrayList<>();
        for (TrackedQuery query : summaries.subList(0, numToReturn)) {
            page.add(toSummary(query, tableNamesById));
        }
        long ttlDays = instanceProperties.getLong(QUERY_TRACKER_ITEM_TTL_IN_DAYS);
        return new QueriesResponse(page, limit, numToReturn < total, ttlDays);
    }

    private enum StateFilter {
        ALL {
            boolean matches(TrackedQuery query) {
                return true;
            }
        },
        QUEUED {
            boolean matches(TrackedQuery query) {
                return query.getLastKnownState() == QueryState.QUEUED;
            }
        },
        IN_PROGRESS {
            boolean matches(TrackedQuery query) {
                return query.getLastKnownState() == QueryState.IN_PROGRESS;
            }
        },
        COMPLETED {
            boolean matches(TrackedQuery query) {
                return query.getLastKnownState() == QueryState.COMPLETED;
            }
        },
        FAILED {
            boolean matches(TrackedQuery query) {
                return query.getLastKnownState() == QueryState.FAILED;
            }
        },
        PARTIALLY_FAILED {
            boolean matches(TrackedQuery query) {
                return query.getLastKnownState() == QueryState.PARTIALLY_FAILED;
            }
        };

        abstract boolean matches(TrackedQuery query);

        static StateFilter parse(String value) {
            if (value == null || value.isBlank()) {
                return ALL;
            }
            try {
                return StateFilter.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return ALL;
            }
        }
    }

    @GET
    @Path("/query/{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryDetail getQuery(@PathParam("queryId") String queryId) {
        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Map<String, String> tableNamesById = tableNamesById(dynamoDbClient, instanceProperties);

        TrackedQuery parent = null;
        List<SubQueryView> subQueries = new ArrayList<>();
        for (TrackedQuery query : tracker.getQueriesWithId(queryId)) {
            if (NON_NESTED_QUERY_PLACEHOLDER.equals(query.getSubQueryId())) {
                parent = query;
            } else {
                subQueries.add(new SubQueryView(
                        query.getSubQueryId(),
                        query.getLastKnownState().name(),
                        toMillis(query.getFirstUpdateTime()),
                        toMillis(query.getLastUpdateTime()),
                        toMillis(query.getExpiryDate()),
                        query.getRowCount(),
                        query.getErrorMessage(),
                        toLocationViews(query.getResultsLocations())));
            }
        }

        if (parent == null) {
            throw notAvailable("query_not_found", "No query found with id " + queryId + ".");
        }

        String tableName = parent.getTableId() == null ? null : tableNamesById.get(parent.getTableId());
        return new QueryDetail(
                parent.getQueryId(),
                parent.getTableId(),
                tableName,
                parent.getLastKnownState().name(),
                toMillis(parent.getFirstUpdateTime()),
                toMillis(parent.getLastUpdateTime()),
                toMillis(parent.getExpiryDate()),
                parent.getRowCount(),
                parent.getErrorMessage(),
                subQueries);
    }

    @GET
    @Path("/query/{queryId}/{subQueryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public SubQueryDetail getSubQuery(
            @PathParam("queryId") String queryId,
            @PathParam("subQueryId") String subQueryId) {
        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);

        TrackedQuery tracked;
        try {
            tracked = tracker.getStatus(queryId, subQueryId);
        } catch (Exception e) {
            throw new WebApplicationException("Could not look up query: " + e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
        if (tracked == null) {
            throw notAvailable("query_not_found", "No query found with id " + queryId + " and sub-query id " + subQueryId + ".");
        }

        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        String tableName = tracked.getTableId() == null ? null : tableIndex.getTableByUniqueId(tracked.getTableId()).map(TableStatus::getTableName).orElse(null);

        return new SubQueryDetail(
                tracked.getQueryId(),
                tracked.getSubQueryId(),
                tracked.getTableId(),
                tableName,
                tracked.getLastKnownState().name(),
                toMillis(tracked.getFirstUpdateTime()),
                toMillis(tracked.getLastUpdateTime()),
                toMillis(tracked.getExpiryDate()),
                tracked.getRowCount(),
                tracked.getErrorMessage(),
                toLocationViews(tracked.getResultsLocations()),
                MAX_RESULTS_LIMIT);
    }

    @GET
    @Path("/query/{queryId}/{subQueryId}/results")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResults getResults(
            @PathParam("queryId") String queryId,
            @PathParam("subQueryId") String subQueryId,
            @QueryParam("location") String location,
            @QueryParam("limit") Integer limitParam) {
        if (location == null || location.isBlank()) {
            throw new WebApplicationException("Request must include a location", Response.Status.BAD_REQUEST);
        }
        int limit = limitParam == null ? DEFAULT_RESULTS_LIMIT : Math.min(Math.max(1, limitParam), MAX_RESULTS_LIMIT);

        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        TrackedQuery tracked;
        try {
            tracked = tracker.getStatus(queryId, subQueryId);
        } catch (Exception e) {
            throw new WebApplicationException("Could not look up query: " + e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }
        if (tracked == null || tracked.getTableId() == null) {
            throw notAvailable("query_not_found", "No query found with id " + queryId + " and sub-query id " + subQueryId + ".");
        }

        Optional<String> s3Path = tracked.getResultsLocations().stream()
                .filter(recorded -> "s3".equals(recorded.getType()))
                .map(ResultsOutputLocation::getLocation)
                .filter(path -> path.equals(location))
                .findFirst();

        Configuration conf = HadoopConfigurationProvider.getConfigurationForClient(instanceProperties);
        List<Map<String, Object>> rows = new ArrayList<>();
        boolean truncated = false;

        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(s3Path.orElseThrow(() ->
                notAvailable("results_location_not_found", "No results file was recorded for this query.")));

        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient);
        Schema schema = tablePropertiesProvider.getById(tracked.getTableId()).getSchema();
        List<String> fieldNames = schema.getAllFields().stream().map(Field::getName).toList();

        try (ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).withConf(conf).build()) {
            Row row;
            while ((row = reader.read()) != null) {
                if (rows.size() >= limit) {
                    truncated = true;
                    break;
                }
                rows.add(toDisplayRow(row, fieldNames));
            }
        } catch (IOException e) {
            throw new WebApplicationException("Could not read query results: " + e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
        }

        return new QueryResults(fieldNames, rows, truncated);
    }

    @POST
    @Path("/query/submit")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(SubmitRequest request) {
        if (request == null || request.tableId() == null || request.tableId().isBlank()) {
            throw new WebApplicationException("Request must include a tableId", Response.Status.BAD_REQUEST);
        }
        if (request.conditions() == null || request.conditions().isEmpty()) {
            throw new WebApplicationException("Request must include at least one condition", Response.Status.BAD_REQUEST);
        }

        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient);
        TableProperties tableProperties;
        try {
            tableProperties = tablePropertiesProvider.getById(request.tableId());
        } catch (RuntimeException e) {
            throw new WebApplicationException("Table not found: " + request.tableId(), Response.Status.BAD_REQUEST);
        }

        Schema schema = tableProperties.getSchema();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Map<String, Field> rowKeyFields = new HashMap<>();
        for (Field field : schema.getRowKeyFields()) {
            rowKeyFields.put(field.getName(), field);
        }

        // Each condition becomes its own region. A region only permits one range per row key field, and
        // the query returns rows matching any of its regions, so multiple conditions form a union.
        List<Region> regions = new ArrayList<>();
        for (Condition condition : request.conditions()) {
            Field field = rowKeyFields.get(condition.field());
            if (field == null) {
                throw new WebApplicationException("Unknown row key field: " + condition.field(), Response.Status.BAD_REQUEST);
            }

            PrimitiveType type = (PrimitiveType) field.getType();
            Object min = parseValue(type, condition.min());
            Object max = parseValue(type, condition.max());
            if (min == null) {
                throw new WebApplicationException("Condition for field " + condition.field() + " must include a min value",
                        Response.Status.BAD_REQUEST);
            }
            boolean minInclusive = condition.minInclusive() == null || condition.minInclusive();
            Range range;
            if (max == null) {
                range = rangeFactory.createExactRange(field, min);
            } else {
                boolean maxInclusive = condition.maxInclusive() != null && condition.maxInclusive();
                range = rangeFactory.createRange(field, min, minInclusive, max, maxInclusive);
            }
            regions.add(new Region(range));
        }

        Map<String, String> resultsPublisherConfig = new HashMap<>();
        resultsPublisherConfig.put(ResultsOutput.DESTINATION, S3ResultsOutput.S3);

        Query query = Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .tableId(request.tableId())
                .regions(regions)
                .processingConfig(QueryProcessingConfig.builder()
                        .requestedValueFields(request.valueFields())
                        .resultsPublisherConfig(resultsPublisherConfig)
                        .build())
                .build();

        QuerySerDe querySerDe = new QuerySerDe(tablePropertiesProvider);
        sqsClient.sendMessage(send -> send.queueUrl(instanceProperties.get(QUERY_QUEUE_URL))
                .messageBody(querySerDe.toJson(query)));

        return Response.status(Response.Status.CREATED)
                .entity(new SubmitResponse(query.getQueryId()))
                .build();
    }

    @GET
    @Path("/query/resources")
    @Produces(MediaType.APPLICATION_JSON)
    public ResourcesResponse getResources() {
        InstanceProperties instanceProperties = loadPropertiesAndCheckEnabled();

        String queryFunctionName = String.join("-", "sleeper", instanceProperties.cleanInstanceId(), "query-executor");
        String subQueryFunctionName = String.join("-", "sleeper", instanceProperties.cleanInstanceId(), "query-leaf-partition");
        String failureFunctionName = String.join("-", "sleeper", instanceProperties.cleanInstanceId(), "query-leaf-partition-failure");

        Map<String, Resource> resources = new HashMap<>();
        resources.put("queryQueue", sqsQueue(sqsClient, instanceProperties.get(QUERY_QUEUE_URL), instanceProperties.get(QUERY_QUEUE_ARN), false));
        resources.put("queryDLQ", sqsQueue(sqsClient, instanceProperties.get(QUERY_DLQ_URL), instanceProperties.get(QUERY_DLQ_ARN), true));
        resources.put("queryFunction", lambdaFunction(lambdaClient, cloudWatchClient, queryFunctionName));
        resources.put("subQueryQueue", sqsQueue(sqsClient, instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_URL), instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_ARN), false));
        resources.put("failureQueue", sqsQueue(sqsClient, instanceProperties.get(LEAF_PARTITION_QUERY_FAILURE_QUEUE_URL), instanceProperties.get(LEAF_PARTITION_QUERY_FAILURE_QUEUE_ARN), true));
        resources.put("failureFunction", lambdaFunction(lambdaClient, cloudWatchClient, failureFunctionName));
        resources.put("subQueryDLQ", sqsQueue(sqsClient, instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_DLQ_URL), instanceProperties.get(LEAF_PARTITION_QUERY_QUEUE_DLQ_ARN), true));
        resources.put("subQueryFunction", lambdaFunction(lambdaClient, cloudWatchClient, subQueryFunctionName));
        resources.put("resultsBucket", s3Bucket(instanceProperties.get(QUERY_RESULTS_BUCKET)));
        resources.put("trackerTable", dynamoTable(dynamoDbClient, instanceProperties.get(QUERY_TRACKER_TABLE_NAME)));
        return new ResourcesResponse(resources);
    }

    private InstanceProperties loadPropertiesAndCheckEnabled() {
        return loadPropertiesAndCheckStackEnabled(s3Client, accountName, instanceId, OptionalStack.QueryStack);
    }

    private static QuerySummary toSummary(TrackedQuery query, Map<String, String> tableNamesById) {
        String tableName = query.getTableId() == null ? null : tableNamesById.get(query.getTableId());
        return new QuerySummary(
                query.getQueryId(),
                query.getTableId(),
                tableName,
                query.getLastKnownState().name(),
                toMillis(query.getLastUpdateTime()),
                toMillis(query.getExpiryDate()),
                query.getRowCount(),
                query.getErrorMessage());
    }

    private static Long toMillis(Long epochSeconds) {
        return epochSeconds == null ? null : epochSeconds * 1000;
    }

    private static List<ResultsLocationView> toLocationViews(List<ResultsOutputLocation> locations) {
        if (locations == null) {
            return List.of();
        }
        return locations.stream()
                .map(location -> new ResultsLocationView(location.getType(), location.getLocation()))
                .toList();
    }

    // Convert a row into JSON-friendly display values (e.g. base64-encode byte arrays).
    private static Map<String, Object> toDisplayRow(Row row, List<String> fieldNames) {
        Map<String, Object> display = new LinkedHashMap<>();
        for (String fieldName : fieldNames) {
            Object value = row.get(fieldName);
            if (value instanceof byte[]) {
                display.put(fieldName, Base64.getEncoder().encodeToString((byte[]) value));
            } else {
                display.put(fieldName, value);
            }
        }
        return display;
    }

    private static Object parseValue(PrimitiveType type, String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        if (type instanceof IntType) {
            return Integer.parseInt(value);
        } else if (type instanceof LongType) {
            return Long.parseLong(value);
        } else if (type instanceof StringType) {
            return value;
        } else if (type instanceof ByteArrayType) {
            return Base64.getDecoder().decode(value);
        } else {
            throw new WebApplicationException("Unsupported row key type: " + type, Response.Status.BAD_REQUEST);
        }
    }

    public record QuerySummary(
            String queryId, String tableId, String tableName, String state,
            Long lastUpdateTime, Long expiryDate, Long rowCount, String errorMessage) {
    }

    public record QueriesResponse(List<QuerySummary> queries, int limit, boolean hasMore, long queryTrackerTtlDays) {}

    public record ResultsLocationView(String type, String location) {}

    public record SubQueryView(
            String subQueryId, String state, Long firstUpdateTime, Long lastUpdateTime, Long expiryDate,
            Long rowCount, String errorMessage, List<ResultsLocationView> resultsLocations) {
    }

    public record QueryDetail(
            String queryId, String tableId, String tableName, String state, Long firstUpdateTime, Long lastUpdateTime,
            Long expiryDate, Long rowCount, String errorMessage, List<SubQueryView> subQueries) {
    }

    public record SubQueryDetail(
            String queryId, String subQueryId, String tableId, String tableName, String state,
            Long firstUpdateTime, Long lastUpdateTime, Long expiryDate, Long rowCount, String errorMessage,
            List<ResultsLocationView> resultsLocations, int maxResultRows) {
    }

    public record QueryResults(List<String> columns, List<Map<String, Object>> rows, boolean truncated) {}
    public record Condition(String field, String min, Boolean minInclusive, String max, Boolean maxInclusive) {}
    public record SubmitRequest(String tableId, List<Condition> conditions, List<String> valueFields) {}
    public record SubmitResponse(String queryId) {}

}
