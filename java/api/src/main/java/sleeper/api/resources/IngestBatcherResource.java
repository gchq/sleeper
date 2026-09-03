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
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.api.AWSArchitectureResources.Resource;
import sleeper.api.AWSArchitectureResources.ResourcesResponse;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.properties.table.TablePropertyGroup;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.IngestBatcherStoreFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static sleeper.api.AWSArchitectureResources.dynamoTable;
import static sleeper.api.AWSArchitectureResources.eventBridgeRule;
import static sleeper.api.AWSArchitectureResources.lambdaFunction;
import static sleeper.api.AWSArchitectureResources.sqsQueue;
import static sleeper.api.ResourceUtils.loadPropertiesAndCheckStackEnabled;
import static sleeper.api.ResourceUtils.notAvailable;
import static sleeper.api.ResourceUtils.tableNamesById;
import static sleeper.core.properties.instance.BatcherProperty.INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;

@Path("/api/ingest-batcher")
public class IngestBatcherResource {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 1000;

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;
    private final CloudWatchClient cloudWatchClient;
    private final EventBridgeClient eventBridgeClient;
    private final LambdaClient lambdaClient;
    private final String instanceId;
    private final String accountName;

    @Inject
    public IngestBatcherResource(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            SqsClient sqsClient,
            CloudWatchClient cloudWatchClient,
            EventBridgeClient eventBridgeClient,
            LambdaClient lambdaClient,
            @ConfigProperty(name = "sleeper.instance.id") String instanceId,
            @ConfigProperty(name = "sleeper.account.name") String accountName) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.sqsClient = sqsClient;
        this.cloudWatchClient = cloudWatchClient;
        this.eventBridgeClient = eventBridgeClient;
        this.lambdaClient = lambdaClient;
        this.instanceId = instanceId;
        this.accountName = accountName;
    }

    @GET
    @Path("/files")
    @Produces(MediaType.APPLICATION_JSON)
    public BatcherFilesResponse getFiles(
            @QueryParam("mode") String mode,
            @QueryParam("tableId") String tableId,
            @QueryParam("path") String path,
            @QueryParam("limit") Integer limitParam) {
        InstanceProperties instanceProperties = loadPropertiesAndCheckStackEnabled(s3Client, accountName, instanceId, OptionalStack.IngestBatcherStack);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient);
        IngestBatcherStore store = getStoreOrThrow(instanceProperties, tablePropertiesProvider);

        int limit = limitParam == null ? DEFAULT_LIMIT : Math.min(Math.max(1, limitParam), MAX_LIMIT);

        List<IngestBatcherTrackedFile> all = "all".equalsIgnoreCase(mode)
                ? store.getAllFilesNewestFirst()
                : store.getPendingFilesOldestFirst();

        String pathFilter = path == null ? null : path.trim().toLowerCase(Locale.ROOT);
        List<IngestBatcherTrackedFile> filtered = all.stream()
                .filter(file -> tableId == null || tableId.isBlank() || tableId.equals(file.getTableId()))
                .filter(file -> pathFilter == null || pathFilter.isEmpty()
                        || file.getFile().toLowerCase(Locale.ROOT).contains(pathFilter))
                .collect(Collectors.toList());

        Map<String, String> tableNamesById = tableNamesById(dynamoDbClient, instanceProperties);

        int total = filtered.size();
        int to = Math.min(limit, total);
        List<BatcherFile> page = filtered.subList(0, to).stream()
                .map(file -> new BatcherFile(
                        file.getFile(),
                        file.getFileSizeBytes(),
                        file.getTableId(),
                        tableNamesById.get(file.getTableId()),
                        file.getReceivedTime().toString(),
                        file.getJobId()))
                .collect(Collectors.toList());

        boolean hasMore = to < total;
        return new BatcherFilesResponse(page, limit, hasMore);
    }

    @GET
    @Path("/config")
    @Produces(MediaType.APPLICATION_JSON)
    public BatchConfig getBatchConfig(@QueryParam("tableId") String tableId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        String jobCreationPeriodMinutes = instanceProperties.get(INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES);

        if (tableId == null || tableId.isBlank()) {
            return new BatchConfig(
                    null,
                    instanceProperties.get(DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE),
                    instanceProperties.get(DEFAULT_INGEST_BATCHER_MAX_JOB_SIZE),
                    instanceProperties.get(DEFAULT_INGEST_BATCHER_MIN_JOB_FILES),
                    instanceProperties.get(DEFAULT_INGEST_BATCHER_MAX_JOB_FILES),
                    instanceProperties.get(DEFAULT_INGEST_BATCHER_MAX_FILE_AGE_SECONDS),
                    instanceProperties.get(DEFAULT_INGEST_BATCHER_INGEST_QUEUE),
                    jobCreationPeriodMinutes,
                    false);
        }

        TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient).getById(tableId);

        boolean defaultsOverridden = TableProperty.getAllInGroup(TablePropertyGroup.INGEST_BATCHER).stream()
                .anyMatch(tableProperties::isSet);

        return new BatchConfig(
                tableId,
                tableProperties.get(INGEST_BATCHER_MIN_JOB_SIZE),
                tableProperties.get(INGEST_BATCHER_MAX_JOB_SIZE),
                tableProperties.get(INGEST_BATCHER_MIN_JOB_FILES),
                tableProperties.get(INGEST_BATCHER_MAX_JOB_FILES),
                tableProperties.get(INGEST_BATCHER_MAX_FILE_AGE_SECONDS),
                tableProperties.get(INGEST_BATCHER_INGEST_QUEUE),
                jobCreationPeriodMinutes,
                defaultsOverridden);
    }

    @GET
    @Path("/resources")
    @Produces(MediaType.APPLICATION_JSON)
    public ResourcesResponse getResources() {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient);
        getStoreOrThrow(instanceProperties, tablePropertiesProvider);

        String submitQueueUrl = instanceProperties.get(INGEST_BATCHER_SUBMIT_QUEUE_URL);
        String submitQueueArn = instanceProperties.get(INGEST_BATCHER_SUBMIT_QUEUE_ARN);
        String dlqUrl = instanceProperties.get(INGEST_BATCHER_SUBMIT_DLQ_URL);
        String dlqArn = instanceProperties.get(INGEST_BATCHER_SUBMIT_DLQ_ARN);
        String submitterName = instanceProperties.get(INGEST_BATCHER_SUBMIT_REQUEST_FUNCTION);
        String tableName = DynamoDBIngestBatcherStore.ingestRequestsTableName(instanceProperties.get(ID));
        String ruleName = instanceProperties.get(INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE);
        String jobCreatorName = instanceProperties.get(INGEST_BATCHER_JOB_CREATION_FUNCTION);

        Map<String, Resource> resources = new HashMap<>();
        resources.put("submitQueue", sqsQueue(sqsClient, submitQueueUrl, submitQueueArn, false));
        resources.put("submitDLQ", sqsQueue(sqsClient, dlqUrl, dlqArn, true));
        resources.put("submitterLambda", lambdaFunction(lambdaClient, cloudWatchClient, submitterName));
        resources.put("trackingTable", dynamoTable(dynamoDbClient, tableName));
        resources.put("creationScheduler", eventBridgeRule(eventBridgeClient, ruleName));
        resources.put("jobCreatorLambda", lambdaFunction(lambdaClient, cloudWatchClient, jobCreatorName));
        resources.put("ingestTarget", new Resource("Sleeper::Ingest", "Ingest", null, null, null, "ok", null));

        return new ResourcesResponse(resources);
    }

    private IngestBatcherStore getStoreOrThrow(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        Optional<IngestBatcherStore> store = IngestBatcherStoreFactory.getStore(dynamoDbClient, instanceProperties, tablePropertiesProvider);
        return store.orElseThrow(() -> notAvailable("ingest_batcher_not_enabled", "The ingest batcher is not enabled for this instance."));
    }

    public record BatcherFile(String file, long fileSizeBytes, String tableId, String tableName, String receivedTime, String jobId) {}
    public record BatcherFilesResponse(List<BatcherFile> files, int limit, boolean hasMore) {}
    public record BatchConfig(String tableId, String minJobSize, String maxJobSize, String minJobFiles, String maxJobFiles, String maxFileAgeSeconds, String ingestQueue, String jobCreationPeriodMinutes, boolean defaultsOverridden) {}

}