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
package sleeper.systemtest.drivers.instance;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@WireMockTest
class AwsSleeperTablesDriverWiremockIT {

    @Test
    void shouldAddOneTable(WireMockRuntimeInfo runtimeInfo) {
        // Given
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(REST_API_URL, runtimeInfo.getHttpBaseUrl());
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, "test-table");
        tableProperties.setSchema(DEFAULT_SCHEMA);
        AwsSleeperTablesDriver driver = new AwsSleeperTablesDriver(SystemTestClients.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key")))
                .build());
        stubFor(post("/sleeper/tables")
                .willReturn(aResponse()
                        .withStatus(201)
                        .withBody("""
                                {
                                    "tableId": "test-id",
                                    "tableName": "test-table"
                                }
                                """)));

        // When the driver is invoked
        driver.addTable(instanceProperties, tableProperties);

        // Then the REST API is called
        verify(postRequestedFor(urlEqualTo("/sleeper/tables"))
                .withHeader("Authorization", matching("AWS4-HMAC-SHA256 .*"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withRequestBody(matchingJsonPath("$.properties['sleeper.table.name']", equalTo("test-table")))
                .withRequestBody(matchingJsonPath("$.schema.rowKeyFields[0].name", equalTo("key")))
                .withRequestBody(matchingJsonPath("$.splitPoints", equalToJson("[]"))));
        // And the table ID is recorded in the TableProperties object
        assertThat(tableProperties.get(TABLE_ID)).isEqualTo("test-id");
    }
}
