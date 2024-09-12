/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.testutil.FileIngestRequestTestHelper;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.test.DynamoDBWiremockTestHelper.wiremockDynamoDBClient;
import static sleeper.dynamodb.test.DynamoDBWiremockTestHelper.wiremockDynamoDBClientBuilder;

@WireMockTest
public class DynamoDBIngestBatcherStoreWiremockIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties table = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = table.get(TABLE_ID);
    private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(table);
    private final FileIngestRequestTestHelper requests = new FileIngestRequestTestHelper();

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

        List<FileIngestRequest> fileIngestRequests = IntStream.range(0, 100)
                .mapToObj(i -> fileRequest().tableId(tableId).build())
                .collect(Collectors.toUnmodifiableList());

        // When
        List<String> assignedFiles = storeWithNoRetry(runtimeInfo).assignJobGetAssigned("test-job", fileIngestRequests);

        // Then
        verify(2, writeItemsRequested());
        assertThat(assignedFiles)
                .containsExactlyElementsOf(fileIngestRequests.subList(0, 50).stream()
                        .map(FileIngestRequest::getFile)
                        .collect(Collectors.toUnmodifiableList()));
    }

    private RequestPatternBuilder writeItemsRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"));
    }

    private IngestBatcherStore store(WireMockRuntimeInfo runtimeInfo) {
        return new DynamoDBIngestBatcherStore(
                wiremockDynamoDBClient(runtimeInfo), instanceProperties, tablePropertiesProvider);
    }

    private IngestBatcherStore storeWithNoRetry(WireMockRuntimeInfo runtimeInfo) {
        return new DynamoDBIngestBatcherStore(
                wiremockDynamoDBClientBuilder(runtimeInfo)
                        .withClientConfiguration(new ClientConfiguration()
                                .withRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY))
                        .build(),
                instanceProperties, tablePropertiesProvider);
    }

    private FileIngestRequest.Builder fileRequest() {
        return requests.fileRequest();
    }
}
