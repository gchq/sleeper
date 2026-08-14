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
import jakarta.ws.rs.core.MediaType;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;

@Path("/api")
public class DataResource {

    private static final String S3_NAMESPACE = "AWS/S3";
    private static final String REQUEST_METRICS_FILTER_ID = "all";
    private static final int DAY_SECONDS = 24 * 60 * 60;

    private static final List<String> REQUEST_METRICS = Arrays.asList(
            "HeadRequests", "GetRequests", "PutRequests", "PostRequests", "DeleteRequests",
            "BytesDownloaded", "BytesUploaded",
            "4xxErrors", "5xxErrors");

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
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        String bucketName = instanceProperties.get(DATA_BUCKET);
        String region = instanceProperties.get(REGION);
        String consoleUrl = String.format(
                "https://s3.console.aws.amazon.com/s3/buckets/%s?region=%s",
                bucketName, region);

        Instant end = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        Instant windowStart = end.minus(Duration.ofHours(24));
        Instant previousWindowStart = end.minus(Duration.ofHours(48));

        Map<String, Double> storageCurrent = queryStorageMetrics(bucketName, windowStart, end);
        Map<String, Double> storagePrevious = queryStorageMetrics(bucketName, previousWindowStart, windowStart);
        Map<String, Double> requestsCurrent = queryRequestMetrics(bucketName, windowStart, end);
        Map<String, Double> requestsPrevious = queryRequestMetrics(bucketName, previousWindowStart, windowStart);

        return new DataMetrics(
                bucketName,
                region,
                consoleUrl,
                compare(storageCurrent, storagePrevious, "BucketSizeBytes"),
                compare(storageCurrent, storagePrevious, "NumberOfObjects"),
                new RequestMetrics(
                        compare(requestsCurrent, requestsPrevious, "HeadRequests"),
                        compare(requestsCurrent, requestsPrevious, "GetRequests"),
                        compare(requestsCurrent, requestsPrevious, "PutRequests"),
                        compare(requestsCurrent, requestsPrevious, "PostRequests"),
                        compare(requestsCurrent, requestsPrevious, "DeleteRequests")),
                compare(requestsCurrent, requestsPrevious, "BytesDownloaded"),
                compare(requestsCurrent, requestsPrevious, "BytesUploaded"),
                new ErrorMetrics(
                        compare(requestsCurrent, requestsPrevious, "4xxErrors"),
                        compare(requestsCurrent, requestsPrevious, "5xxErrors")));
    }

    private Map<String, Double> queryStorageMetrics(String bucketName, Instant start, Instant end) {
        List<MetricDataQuery> queries = List.of(
                storageQuery("bucketSizeBytes", "BucketSizeBytes", bucketName, "StandardStorage"),
                storageQuery("numberOfObjects", "NumberOfObjects", bucketName, "AllStorageTypes"));
        Map<String, String> idToName = Map.of(
                "bucketSizeBytes", "BucketSizeBytes",
                "numberOfObjects", "NumberOfObjects");
        return runQueries(queries, idToName, start.minus(Duration.ofHours(24)), end);
    }

    private Map<String, Double> queryRequestMetrics(String bucketName, Instant start, Instant end) {
        List<MetricDataQuery> queries = new ArrayList<>();
        Map<String, String> idToName = new HashMap<>();
        for (int i = 0; i < REQUEST_METRICS.size(); i++) {
            String metricName = REQUEST_METRICS.get(i);
            String id = "m" + i;
            queries.add(requestQuery(id, metricName, bucketName));
            idToName.put(id, metricName);
        }
        return runQueries(queries, idToName, start, end);
    }

    private Map<String, Double> runQueries(
            List<MetricDataQuery> queries, Map<String, String> idToName,
            Instant start, Instant end) {
        GetMetricDataResponse response = cloudWatchClient.getMetricData(builder -> builder
                .startTime(start)
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

    private static MetricDataQuery storageQuery(String id, String metricName, String bucketName, String storageType) {
        return MetricDataQuery.builder()
                .id(id)
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(S3_NAMESPACE)
                                .metricName(metricName)
                                .dimensions(
                                        Dimension.builder().name("BucketName").value(bucketName).build(),
                                        Dimension.builder().name("StorageType").value(storageType).build())
                                .build())
                        .stat("Average")
                        .period(DAY_SECONDS)
                        .build())
                .build();
    }

    private static MetricDataQuery requestQuery(String id, String metricName, String bucketName) {
        return MetricDataQuery.builder()
                .id(id)
                .metricStat(MetricStat.builder()
                        .metric(Metric.builder()
                                .namespace(S3_NAMESPACE)
                                .metricName(metricName)
                                .dimensions(
                                        Dimension.builder().name("BucketName").value(bucketName).build(),
                                        Dimension.builder().name("FilterId").value(REQUEST_METRICS_FILTER_ID).build())
                                .build())
                        .stat("Sum")
                        .period(DAY_SECONDS)
                        .build())
                .build();
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

}
