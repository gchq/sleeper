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
package sleeper.statestore.transactionlog.snapshots;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
public class DynamoDBTransactionLogSnapshotMetadataStoreWiremockIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    @Test
    void shouldNotSwallowExceptionsOnFailedSaveSnapshot(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(post("/").withHeader("X-Amz-Target", equalTo("DynamoDB_20120810.TransactWriteItems"))
                .willReturn(aResponse().withStatus(400).withBody("""
                        {"__type": "TransactionCanceledException", "message": "Test failure", "CancellationReasons": [{"Code": "ThrottlingError"}]}
                        """)));

        // When / Then
        assertThatThrownBy(() -> store(runtimeInfo).saveSnapshot(filesSnapshot(1)))
                .isInstanceOf(TransactionCanceledException.class)
                .hasMessageStartingWith("Test failure");
    }

    private DynamoDBTransactionLogSnapshotMetadataStore store(WireMockRuntimeInfo runtimeInfo) {
        return new DynamoDBTransactionLogSnapshotMetadataStore(
                instanceProperties, tableProperties, wiremockAwsV2Client(runtimeInfo, DynamoDbClient.builder()));
    }

    private TransactionLogSnapshotMetadata filesSnapshot(long transactionNumber) {
        return TransactionLogSnapshotMetadata.forFiles(tableProperties.get(TABLE_ID), transactionNumber);
    }
}
