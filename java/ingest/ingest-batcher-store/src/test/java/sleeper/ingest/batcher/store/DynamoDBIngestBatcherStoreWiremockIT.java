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

package sleeper.ingest.batcher.store;

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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.tools.DynamoDBWiremockTestHelper.wiremockDynamoDBClient;

@WireMockTest
public class DynamoDBIngestBatcherStoreWiremockIT {

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties table = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    protected final String tableName = table.get(TABLE_NAME);
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
        store(runtimeInfo).assignJob("test-job", List.of(fileRequest().tableName(tableName).build()));

        // Then
        verify(2, writeItemsRequested());
    }

    private RequestPatternBuilder writeItemsRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"));
    }

    private IngestBatcherStore store(WireMockRuntimeInfo runtimeInfo) {
        return new DynamoDBIngestBatcherStore(
                wiremockDynamoDBClient(runtimeInfo), instanceProperties, tablePropertiesProvider);
    }

    private FileIngestRequest.Builder fileRequest() {
        return requests.fileRequest();
    }
}
