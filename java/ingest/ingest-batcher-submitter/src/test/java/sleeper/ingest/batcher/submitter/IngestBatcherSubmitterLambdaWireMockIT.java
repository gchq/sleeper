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
package sleeper.ingest.batcher.submitter;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.testutil.InMemoryIngestBatcherStore;

import java.time.Instant;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.findUnmatchedRequests;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2ClientWithRetryAttempts;

@WireMockTest
public class IngestBatcherSubmitterLambdaWireMockIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private final IngestBatcherStore ingestBatcherStore = new InMemoryIngestBatcherStore();

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_ID, "test-table-id");
        tableProperties.set(TABLE_NAME, "test-table");
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName("test-table-id", "test-table"));
    }

    @Test
    void shouldHandleNetworkErrorCorrectly(WireMockRuntimeInfo runtimeInfo) {
        // Given
        // Mock Network error
        stubFor(get("/test-bucket?list-type=2&prefix=test-file-1.parquet")
                .willReturn(aResponse().withStatus(500)));

        // Define job
        String json = "{" +
                "\"files\":[\"test-bucket/test-file-1.parquet\"]," +
                "\"tableName\":\"test-table\"" +
                "}";
        IngestBatcherSubmitterLambda lambda = lambda(runtimeInfo);

        // When / Then
        assertThatThrownBy(() -> lambda.handleMessage(json, Instant.parse("2023-06-16T10:57:00Z")))
                .isInstanceOfSatisfying(S3Exception.class,
                        e -> assertThat(e.statusCode()).isEqualTo(500));
        assertThat(findUnmatchedRequests()).isEmpty();
        assertThat(ingestBatcherStore.getAllFilesNewestFirst()).isEmpty();
    }

    private IngestBatcherSubmitterLambda lambda(WireMockRuntimeInfo runtimeInfo) {
        S3Client s3Client = wiremockAwsV2Client(runtimeInfo, S3Client.builder().forcePathStyle(true));
        SqsClient sqsClient = wiremockAwsV2ClientWithRetryAttempts(10, runtimeInfo, SqsClient.builder());
        return new IngestBatcherSubmitterLambda(
                ingestBatcherStore, instanceProperties, tableIndex,
                new IngestBatcherSubmitDeadLetterQueue(instanceProperties, sqsClient),
                s3Client);
    }
}
