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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.Metric;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataQuery;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;
import software.amazon.awssdk.services.cloudwatch.model.ScanBy;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.api.PropertyUpdates;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.ReadSplitPoints;
import sleeper.core.properties.local.WriteSplitPoints;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.AddTable;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.statestore.StateStoreFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

@Path("/api/tables")
public class TablesResource {

    // Latest RowCount value from the last hour
    private static final int LATEST_PERIOD_SECONDS = 60;
    private static final Duration LATEST_WINDOW = Duration.ofHours(1);

    // RowCount hourly average over the last 7 days
    private static final int ROW_COUNT_PERIOD_SECONDS = 60 * 60;
    private static final Duration ROW_COUNT_WINDOW = Duration.ofDays(7);

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final CloudWatchClient cloudWatchClient;
    private final String instanceId;
    private final String accountName;

    @Inject
    public TablesResource(
        S3Client s3Client,
        DynamoDbClient dynamoDbClient,
        CloudWatchClient cloudWatchClient,
        @ConfigProperty(name = "sleeper.instance.id") String instanceId,
        @ConfigProperty(name = "sleeper.account.name") String accountName
    ) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.cloudWatchClient = cloudWatchClient;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TableStatus> getTables() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        return tableIndex.streamAllTables().collect(Collectors.toList());
    }

    @GET
    @Path("/row-counts")
    @Produces(MediaType.APPLICATION_JSON)
    public RowCountsResponse getRowCounts() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
        List<TableStatus> tables = tableIndex.streamAllTables().collect(Collectors.toList());

        String namespace = instanceProperties.get(METRICS_NAMESPACE);
        Instant endTime = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        Instant sparklineStart = endTime.truncatedTo(ChronoUnit.HOURS).minus(ROW_COUNT_WINDOW);

        // Sparkline: hourly average over the last 7 days.
        Map<String, List<Point>> pointsByTableId = querySeries(
                tables, namespace, sparklineStart, endTime, "Average", ROW_COUNT_PERIOD_SECONDS);
        // Displayed value: the single most recent published RowCount at native resolution.
        Map<String, List<Point>> latestByTableId = querySeries(
                tables, namespace, endTime.minus(LATEST_WINDOW), endTime, "Maximum", LATEST_PERIOD_SECONDS);

        List<TableRowCounts> series = new ArrayList<>();
        for (TableStatus table : tables) {
            String tableId = table.getTableUniqueId();
            series.add(new TableRowCounts(
                    tableId,
                    table.getTableName(),
                    latestPointValue(latestByTableId.get(tableId)),
                    pointsByTableId.get(tableId)));
        }
        return new RowCountsResponse(
                sparklineStart.toString(),
                endTime.toString(),
                ROW_COUNT_PERIOD_SECONDS,
                series);
    }

    private Map<String, List<Point>> querySeries(
            List<TableStatus> tables, String namespace, Instant start, Instant end, String stat, int period) {
        List<MetricDataQuery> queries = new ArrayList<>();
        Map<String, TableStatus> idToTable = new HashMap<>();
        for (int i = 0; i < tables.size(); i++) {
            TableStatus table = tables.get(i);
            String id = "t" + i;
            queries.add(buildRowCountQuery(id, namespace, instanceId, table.getTableName(), stat, period));
            idToTable.put(id, table);
        }

        Map<String, List<Point>> pointsByTableId = new LinkedHashMap<>();
        for (TableStatus table : tables) {
            pointsByTableId.put(table.getTableUniqueId(), new ArrayList<>());
        }
        if (queries.isEmpty()) {
            return pointsByTableId;
        }
        GetMetricDataResponse response = cloudWatchClient.getMetricData(builder -> builder
                .startTime(start)
                .endTime(end)
                .scanBy(ScanBy.TIMESTAMP_ASCENDING)
                .metricDataQueries(queries));
        for (MetricDataResult result : response.metricDataResults()) {
            TableStatus table = idToTable.get(result.id());
            if (table == null) {
                continue;
            }
            List<Instant> timestamps = result.timestamps();
            List<Double> values = result.values();
            if (timestamps == null || values == null) {
                continue;
            }
            List<Point> points = pointsByTableId.get(table.getTableUniqueId());
            int count = Math.min(timestamps.size(), values.size());
            for (int i = 0; i < count; i++) {
                points.add(new Point(timestamps.get(i).toString(), values.get(i)));
            }
        }
        return pointsByTableId;
    }

    private static Double latestPointValue(List<Point> points) {
        if (points == null || points.isEmpty()) {
            return null;
        }
        return points.get(points.size() - 1).value();
    }

    private static MetricDataQuery buildRowCountQuery(
            String id, String namespace, String instanceId, String tableName, String stat, int period) {
        return MetricDataQuery.builder()
                .id(id)
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(namespace)
                                .metricName("RowCount")
                                .dimensions(
                                        Dimension.builder().name("instanceId").value(instanceId).build(),
                                        Dimension.builder().name("tableName").value(tableName).build())
                                .build())
                        .stat(stat)
                        .period(period)
                        .build())
                .build();
    }

    public record Point(String time, Double value) {}
    public record TableRowCounts(String tableUniqueId, String tableName, Double latestRowCount, List<Point> points) {}
    public record RowCountsResponse(String startTime, String endTime, int period, List<TableRowCounts> series) {}

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createTable(CreateTableRequest request) {
        if (request == null || request.schema() == null || request.schema().isBlank()) {
            throw new WebApplicationException("Request must include a schema", Response.Status.BAD_REQUEST);
        }

        Schema schema;
        try {
            schema = new SchemaSerDe().fromJson(request.schema());
        } catch (RuntimeException e) {
            throw new WebApplicationException("Invalid schema: " + e.getMessage(), Response.Status.BAD_REQUEST);
        }

        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        Properties props = new Properties();
        if (request.properties() != null) {
            props.putAll(request.properties());
        }
        TableProperties tableProperties;
        try {
            tableProperties = new TableProperties(instanceProperties, props);
            tableProperties.setSchema(schema);
        } catch (RuntimeException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
        }

        List<String> splitPointLines = request.splitPoints() == null ? List.of() : request.splitPoints();
        List<Object> splitPoints;
        try {
            splitPoints = ReadSplitPoints.fromString(String.join("\n", splitPointLines), schema, false);
        } catch (RuntimeException e) {
            throw new WebApplicationException("Invalid split points: " + e.getMessage(), Response.Status.BAD_REQUEST);
        }

        TablePropertiesStore store = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient);
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDbClient);
        try {
            new AddTable(store, stateStoreProvider).addTable(tableProperties, splitPoints);
        } catch (TableAlreadyExistsException e) {
            throw new WebApplicationException(
                    Response.status(Response.Status.CONFLICT)
                            .entity(new CreateFailure("table_already_exists", e.getMessage()))
                            .type(MediaType.APPLICATION_JSON)
                            .build());
        } catch (IllegalArgumentException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.BAD_REQUEST);
        }
        return Response.status(Response.Status.CREATED)
                .entity(new CreateTableResponse(tableProperties.get(TABLE_ID), tableProperties.get(TABLE_NAME)))
                .build();
    }

    public record CreateTableRequest(Map<String, String> properties, String schema, List<String> splitPoints) {}
    public record CreateTableResponse(String tableId, String tableName) {}
    public record CreateFailure(String error, String message) {}

    @GET
    @Path("/{tableId}/properties")
    @Produces(MediaType.APPLICATION_JSON)
    public Properties getTableProperties(@PathParam("tableId") String tableId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient).getById(tableId);
        return tableProperties.getProperties();
    }

    @GET
    @Path("/{tableId}/split-points")
    @Produces(MediaType.APPLICATION_JSON)
    public SplitPointsResponse getTableSplitPoints(@PathParam("tableId") String tableId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        TableProperties tableProperties;
        try {
            tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient).loadById(tableId);
        } catch (TableNotFoundException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.NOT_FOUND);
        }
        StateStore stateStore = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDbClient)
                .getStateStore(tableProperties);
        List<Object> splitPoints = new PartitionTree(stateStore.getAllPartitions())
                .getSplitPoints(tableProperties.getSchema());
        // Serialise with the same writer the CLI/file flow uses, so values round-trip through the create endpoint.
        String text = WriteSplitPoints.toString(splitPoints, false);
        List<String> lines = text.isEmpty() ? List.of() : List.of(text.split("\n"));
        return new SplitPointsResponse(lines);
    }

    public record SplitPointsResponse(List<String> splitPoints) {}

    @GET
    @Path("/{tableId}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    public SchemaResponse getTableSchema(@PathParam("tableId") String tableId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        TableProperties tableProperties;
        try {
            tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient).loadById(tableId);
        } catch (TableNotFoundException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.NOT_FOUND);
        }
        String schemaJson = new SchemaSerDe().toJson(tableProperties.getSchema());
        return new SchemaResponse(tableProperties.get(TABLE_ID), tableProperties.get(TABLE_NAME), schemaJson);
    }

    public record SchemaResponse(String tableId, String tableName, String schema) {}

    @POST
    @Path("/{tableId}/properties")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTableProperties(@PathParam("tableId") String tableId, Map<String, String> changes) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        TablePropertiesStore store = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient);
        TableProperties tableProperties;
        try {
            tableProperties = store.loadById(tableId);
        } catch (TableNotFoundException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.NOT_FOUND);
        }
        PropertyUpdates.applyOrThrow(tableProperties, changes);
        store.save(tableProperties);
        return Response.noContent().build();
    }

}
