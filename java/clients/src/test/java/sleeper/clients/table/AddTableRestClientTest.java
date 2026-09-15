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

package sleeper.clients.table;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.restapi.addTable.AddTableResponse;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REST_API_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

class AddTableRestClientTest {
    private final WireMockServer wireMock = new WireMockServer(options().dynamicPort());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
            AwsBasicCredentials.create("access-key", "secret-key"));

    @BeforeEach
    void setUp() {
        wireMock.start();
        instanceProperties.set(REST_API_URL, wireMock.baseUrl());
        instanceProperties.set(REGION, "eu-west-2");
    }

    @AfterEach
    void tearDown() {
        wireMock.stop();
    }

    @Test
    void shouldAddTableThroughRestApi() {
        wireMock.stubFor(post("/sleeper/tables")
                .willReturn(aResponse().withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"tableId\":\"table-id\",\"tableName\":\"table-name\"}")));
        TableProperties tableProperties = tableProperties("table-name");

        AddTableResponse response;
        try (AddTableRestClient client = new AddTableRestClient(instanceProperties, credentials)) {
            response = client.addTable(tableProperties);
        }

        assertThat(response).isEqualTo(AddTableResponse.builder()
                .tableId("table-id")
                .tableName("table-name")
                .build());
        wireMock.verify(postRequestedFor(urlEqualTo("/sleeper/tables"))
                .withRequestBody(equalToJson(new sleeper.restapi.addTable.AddTableRequestSerDe(instanceProperties)
                        .toJson(sleeper.restapi.addTable.AddTableRequest.builder()
                                .properties(tableProperties)
                                .build()))));
    }

    @Test
    void shouldReportRestApiError() {
        wireMock.stubFor(post("/sleeper/tables")
                .willReturn(aResponse().withStatus(400).withBody("bad request")));

        try (AddTableRestClient client = new AddTableRestClient(instanceProperties, credentials)) {
            assertThatThrownBy(() -> client.addTable(tableProperties("table-name")))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Failed to add table through REST API, status code: 400, response: bad request");
        }
    }

    private TableProperties tableProperties(String tableName) {
        TableProperties properties = new TableProperties(instanceProperties);
        properties.set(TABLE_NAME, tableName);
        properties.setSchema(createSchemaWithKey("key"));
        return properties;
    }
}
