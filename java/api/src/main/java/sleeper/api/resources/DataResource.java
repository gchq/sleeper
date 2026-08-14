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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
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
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;

@Path("/api")
public class DataResource {

    private static final String S3_NAMESPACE = "AWS/S3";
    private static final String REQUEST_METRICS_FILTER_ID = "all";
    private static final int DAY_SECONDS = 24 * 60 * 60;

    private enum MetricType {
        STORAGE_STANDARD("Average", "StandardStorage"),
        STORAGE_ALL("Average", "AllStorageTypes"),
        REQUEST("Sum", null);

        final String stat;
        final String storageType;

        MetricType(String stat, String storageType) {
            this.stat = stat;
            this.storageType = storageType;
        }
    }

    private static final Map<String, MetricType> METRIC_TYPES = new LinkedHashMap<>();
    static {
        METRIC_TYPES.put("BucketSizeBytes", MetricType.STORAGE_STANDARD);
        METRIC_TYPES.put("NumberOfObjects", MetricType.STORAGE_ALL);
        for (String name : Arrays.asList(
                "HeadRequests", "GetRequests", "PutRequests", "PostRequests", "DeleteRequests",
                "BytesDownloaded", "BytesUploaded",
                "4xxErrors", "5xxErrors")) {
            METRIC_TYPES.put(name, MetricType.REQUEST);
        }
    }

    private final S3Client s3Client;
    private final CloudWatchClient cloudWatchClient;
    private final String instanceId;
    private final String accountName;

    @Inject
    public DataResource(
            S3Client s3Client,
            CloudWatchClient cloudWatchClient,
            @ConfigProperty(name = "sleeper.instance.id") String instanceId,
            @ConfigProperty(name = "sleeper.account.name") String accountName) {
        this.s3Client = s3Client;
        this.cloudWatchClient = cloudWatchClient;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Path("/data")
    @Produces(MediaType.APPLICATION_JSON)
    public DataMetrics getDataMetrics() {
        String bucketName = loadDataBucketName();
        String region = loadRegion();
        String consoleUrl = buildConsoleUrl(bucketName, region);

        Instant end = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        Instant windowStart = end.minus(Duration.ofHours(24));
        Instant previousWindowStart = end.minus(Duration.ofHours(48));

        Map<String, Double> current = latestValues(bucketName, windowStart, end);
        Map<String, Double> previous = latestValues(bucketName, previousWindowStart, windowStart);

        return new DataMetrics(
                bucketName,
                region,
                consoleUrl,
                compare(current, previous, "BucketSizeBytes"),
                compare(current, previous, "NumberOfObjects"),
                new RequestMetrics(
                        compare(current, previous, "HeadRequests"),
                        compare(current, previous, "GetRequests"),
                        compare(current, previous, "PutRequests"),
                        compare(current, previous, "PostRequests"),
                        compare(current, previous, "DeleteRequests")),
                compare(current, previous, "BytesDownloaded"),
                compare(current, previous, "BytesUploaded"),
                new ErrorMetrics(
                        compare(current, previous, "4xxErrors"),
                        compare(current, previous, "5xxErrors")));
    }

    @GET
    @Path("/data/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public MetricsSeries getMetricsSeries(
            @QueryParam("metrics") String metricsParam,
            @QueryParam("startTime") String startTimeParam,
            @QueryParam("endTime") String endTimeParam,
            @QueryParam("period") Integer periodParam) {
        if (metricsParam == null || metricsParam.isBlank()) {
            throw new WebApplicationException("Missing 'metrics' query parameter", Response.Status.BAD_REQUEST);
        }
        Instant startTime = parseInstant("startTime", startTimeParam);
        Instant endTime = parseInstant("endTime", endTimeParam);
        if (!endTime.isAfter(startTime)) {
            throw new WebApplicationException("'endTime' must be after 'startTime'", Response.Status.BAD_REQUEST);
        }
        int period = periodParam == null ? DAY_SECONDS : periodParam;
        if (period <= 0) {
            throw new WebApplicationException("'period' must be positive", Response.Status.BAD_REQUEST);
        }

        List<String> metricNames = Arrays.stream(metricsParam.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
        for (String name : metricNames) {
            if (!METRIC_TYPES.containsKey(name)) {
                throw new WebApplicationException("Unknown metric: " + name, Response.Status.BAD_REQUEST);
            }
        }

        String bucketName = loadDataBucketName();
        List<MetricDataQuery> queries = new ArrayList<>();
        Map<String, String> idToName = new HashMap<>();
        for (int i = 0; i < metricNames.size(); i++) {
            String name = metricNames.get(i);
            String id = "m" + i;
            queries.add(buildQuery(id, name, bucketName, period));
            idToName.put(id, name);
        }
        GetMetricDataResponse response = cloudWatchClient.getMetricData(builder -> builder
                .startTime(startTime)
                .endTime(endTime)
                .scanBy(ScanBy.TIMESTAMP_ASCENDING)
                .metricDataQueries(queries));

        Map<String, MetricSeries> byName = new LinkedHashMap<>();
        for (String name : metricNames) {
            byName.put(name, new MetricSeries(name, new ArrayList<>()));
        }
        for (MetricDataResult result : response.metricDataResults()) {
            String name = idToName.get(result.id());
            if (name == null) continue;
            List<Instant> timestamps = result.timestamps();
            List<Double> values = result.values();
            if (timestamps == null || values == null) continue;
            List<Point> points = byName.get(name).points();
            int count = Math.min(timestamps.size(), values.size());
            for (int i = 0; i < count; i++) {
                points.add(new Point(timestamps.get(i).toString(), values.get(i)));
            }
        }

        return new MetricsSeries(
                startTime.toString(),
                endTime.toString(),
                period,
                new ArrayList<>(byName.values()));
    }

    private String loadDataBucketName() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        return instanceProperties.get(DATA_BUCKET);
    }

    private String loadRegion() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        return instanceProperties.get(REGION);
    }

    private static String buildConsoleUrl(String bucketName, String region) {
        return String.format("https://s3.console.aws.amazon.com/s3/buckets/%s?region=%s", bucketName, region);
    }

    private static Instant parseInstant(String paramName, String value) {
        if (value == null || value.isBlank()) {
            throw new WebApplicationException("Missing '" + paramName + "' query parameter", Response.Status.BAD_REQUEST);
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new WebApplicationException(
                    "'" + paramName + "' is not a valid ISO-8601 instant: " + value,
                    Response.Status.BAD_REQUEST);
        }
    }

    private Map<String, Double> latestValues(String bucketName, Instant start, Instant end) {
        List<MetricDataQuery> queries = new ArrayList<>();
        Map<String, String> idToName = new HashMap<>();
        int i = 0;
        for (String name : METRIC_TYPES.keySet()) {
            String id = "m" + i++;
            queries.add(buildQuery(id, name, bucketName, DAY_SECONDS));
            idToName.put(id, name);
        }
        GetMetricDataResponse response = cloudWatchClient.getMetricData(builder -> builder
                .startTime(start.minus(Duration.ofHours(24)))
                .endTime(end)
                .scanBy(ScanBy.TIMESTAMP_DESCENDING)
                .metricDataQueries(queries));
        Map<String, Double> results = new HashMap<>();
        for (MetricDataResult result : response.metricDataResults()) {
            String name = idToName.get(result.id());
            if (name == null) continue;
            List<Double> values = result.values();
            if (values != null && !values.isEmpty()) {
                results.put(name, values.get(0));
            }
        }
        return results;
    }

    private static MetricDataQuery buildQuery(String id, String metricName, String bucketName, int period) {
        MetricType type = METRIC_TYPES.get(metricName);
        return MetricDataQuery.builder()
                .id(id)
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(S3_NAMESPACE)
                                .metricName(metricName)
                                .dimensions(dimensionsFor(type, bucketName))
                                .build())
                        .stat(type.stat)
                        .period(period)
                        .build())
                .build();
    }

    private static List<Dimension> dimensionsFor(MetricType type, String bucketName) {
        Dimension bucket = Dimension.builder().name("BucketName").value(bucketName).build();
        if (type == MetricType.REQUEST) {
            return List.of(bucket,
                    Dimension.builder().name("FilterId").value(REQUEST_METRICS_FILTER_ID).build());
        }
        return List.of(bucket,
                Dimension.builder().name("StorageType").value(type.storageType).build());
    }

    private static Comparison compare(Map<String, Double> current, Map<String, Double> previous, String name) {
        return new Comparison(current.get(name), previous.get(name));
    }

    public record Comparison(Double current, Double previous) {
    }

    public record RequestMetrics(
            Comparison head,
            Comparison get,
            Comparison put,
            Comparison post,
            Comparison delete) {
    }

    public record ErrorMetrics(
            Comparison status4xx,
            Comparison status5xx) {
    }

    public record DataMetrics(
            String bucketName,
            String region,
            String consoleUrl,
            Comparison bucketSizeBytes,
            Comparison numberOfObjects,
            RequestMetrics requests,
            Comparison bytesDownloaded,
            Comparison bytesUploaded,
            ErrorMetrics errors) {
    }

    public record Point(String time, Double value) {
    }

    public record MetricSeries(String metric, List<Point> points) {
    }

    public record MetricsSeries(
            String startTime,
            String endTime,
            int period,
            List<MetricSeries> series) {
    }

}
