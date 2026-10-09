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

package sleeper.ingest.batcher.store;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.core.testutil.FileIngestRequestTestHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.testutils.JitterTestHelper.constantJitterFraction;
import static sleeper.core.util.ThreadSleepTestHelper.recordWaits;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.FILE_PATH;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.JOB_ID;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.NOT_ASSIGNED_TO_JOB;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2ClientWithRetryAttempts;

@WireMockTest
public class DynamoDBIngestBatcherStoreWiremockIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties table = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private final String tableId = table.get(TABLE_ID);
    private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(table);
    private final FileIngestRequestTestHelper requests = new FileIngestRequestTestHelper();
    private final List<Duration> foundWaits = new ArrayList<>();

    @Test
    void shouldRetryTransactionOnInternalServerError(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(post("/").withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"))
                .inScenario("retry transaction")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry success")
                .willReturn(aResponse().withStatus(500)));
        stubFor(post("/").withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"))
                .inScenario("retry transaction")
                .whenScenarioStateIs("retry success")
                .willReturn(aResponse().withStatus(200)));

        // When
        store(runtimeInfo).assignJobGetAssigned("test-job", List.of(fileRequest().tableId(tableId).build()));

        // Then
        verify(2, writeItemsRequested());
    }

    @Test
    void shouldAssignFirstBatchAndFailSecondBatchReturningAssignedFiles(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(post("/").withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"))
                .inScenario("fail second batch")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("second batch")
                .willReturn(aResponse().withStatus(200)));
        stubFor(post("/").withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"))
                .inScenario("fail second batch")
                .whenScenarioStateIs("second batch")
                .willReturn(aResponse().withStatus(500)));

        List<IngestBatcherTrackedFile> fileIngestRequests = IntStream.range(0, 100)
                .mapToObj(i -> fileRequest().tableId(tableId).build())
                .collect(Collectors.toUnmodifiableList());

        // When
        List<String> assignedFiles = storeWithNoRetry(runtimeInfo).assignJobGetAssigned("test-job", fileIngestRequests);

        // Then
        verify(2, writeItemsRequested());
        assertThat(assignedFiles)
                .containsExactlyElementsOf(fileIngestRequests.subList(0, 50).stream()
                        .map(IngestBatcherTrackedFile::getFile)
                        .collect(Collectors.toUnmodifiableList()));
    }

    @Test
    void shouldRetryDeleteWhenItemIsUnprocessed(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(batchWriteItem()
                .inScenario("retry unprocessed")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry success")
                .willReturn(aResponse().withStatus(200)
                        .withBody(unprocessedDeletesResponse("file-2.parquet"))));
        stubFor(batchWriteItem()
                .inScenario("retry unprocessed")
                .whenScenarioStateIs("retry success")
                .willReturn(aResponse().withStatus(200).withBody("{}")));

        // When
        store(runtimeInfo).deleteFiles(List.of(
                fileRequest().tableId(tableId).file("file-1.parquet").build(),
                fileRequest().tableId(tableId).file("file-2.parquet").build()));

        // Then
        verify(2, batchWriteItemRequested());
        verify(1, batchWriteItemRequested().withRequestBody(equalToJson(deletesRequest("file-1.parquet", "file-2.parquet"))));
        verify(1, batchWriteItemRequested().withRequestBody(equalToJson(deletesRequest("file-2.parquet"))));
        assertThat(foundWaits).containsExactly(Duration.ofMillis(500));
    }

    @Test
    void shouldFailWhenItemIsStillUnprocessedAfterMaxAttempts(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(batchWriteItem()
                .willReturn(aResponse().withStatus(200)
                        .withBody(unprocessedDeletesResponse("file.parquet"))));
        IngestBatcherStore store = store(runtimeInfo);
        List<IngestBatcherTrackedFile> files = List.of(fileRequest().tableId(tableId).file("file.parquet").build());

        // When / Then
        assertThatThrownBy(() -> store.deleteFiles(files))
                .hasMessage("Failed to delete files, too many unprocessed writes after 10 attempts");
        verify(DynamoDBIngestBatcherStore.MAX_ATTEMPTS_PER_BATCH_WRITE, batchWriteItemRequested());
    }

    private MappingBuilder batchWriteItem() {
        return post("/").withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.BatchWriteItem"));
    }

    private RequestPatternBuilder writeItemsRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"));
    }

    private RequestPatternBuilder batchWriteItemRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.BatchWriteItem"));
    }

    private String deletesRequest(String... files) {
        return "{\"RequestItems\": " + deletesByTable(files) + "}";
    }

    private String unprocessedDeletesResponse(String... files) {
        return "{\"UnprocessedItems\": " + deletesByTable(files) + "}";
    }

    private String deletesByTable(String... files) {
        return "{\"" + DynamoDBIngestBatcherStore.ingestRequestsTableName(instanceProperties.get(ID)) + "\": [" +
                Stream.of(files)
                        .map(file -> "{\"DeleteRequest\": {\"Key\": {" +
                                "\"" + JOB_ID + "\": {\"S\": \"" + NOT_ASSIGNED_TO_JOB + "\"}, " +
                                "\"" + FILE_PATH + "\": {\"S\": \"" + tableId + "/" + file + "\"}}}}")
                        .collect(Collectors.joining(", ")) +
                "]}";
    }

    private IngestBatcherStore store(WireMockRuntimeInfo runtimeInfo) {
        return new DynamoDBIngestBatcherStore(
                wiremockAwsV2Client(runtimeInfo, DynamoDbClient.builder()),
                instanceProperties, tablePropertiesProvider, DynamoDBIngestBatcherStore.FILES_IN_ASSIGN_JOB_BATCH,
                new ExponentialBackoffWithJitter(DynamoDBIngestBatcherStore.UNPROCESSED_WRITES_WAIT_RANGE,
                        constantJitterFraction(0.5), recordWaits(foundWaits)));
    }

    private IngestBatcherStore storeWithNoRetry(WireMockRuntimeInfo runtimeInfo) {
        return new DynamoDBIngestBatcherStore(
                // Passing a 1 into this method means do 1 attempt overall, not 1 attempt and 1 retry.
                // So in effect this method is not re-trying if the initial attempt fails.
                wiremockAwsV2ClientWithRetryAttempts(1, runtimeInfo, DynamoDbClient.builder()),
                instanceProperties, tablePropertiesProvider);
    }

    private IngestBatcherTrackedFile.Builder fileRequest() {
        return requests.fileRequest();
    }
}
