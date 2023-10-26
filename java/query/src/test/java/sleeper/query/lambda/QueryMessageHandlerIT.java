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

package sleeper.query.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.LongType;
import sleeper.dynamodb.tools.DynamoDBContainer;
import sleeper.query.model.Query;
import sleeper.query.tracker.DynamoDBQueryTracker;
import sleeper.query.tracker.DynamoDBQueryTrackerCreator;
import sleeper.query.tracker.QueryState;
import sleeper.query.tracker.TrackedQuery;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class QueryMessageHandlerIT {
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static DynamoDBContainer dynamoDb = new DynamoDBContainer();

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, dynamoDb.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());
    }

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

    @BeforeEach
    void setUp() {
        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoDBClient).create();
    }

    private final TableProperties tableProperties = createTable("table-1");
    private final QueryMessageHandler queryMessageHandler = new QueryMessageHandler(new FixedTablePropertiesProvider(tableProperties),
            queryTracker, () -> "invalid-query-id");

    @Nested
    @DisplayName("Failed to deserialise")
    class FailedToDeserialise {
        @Test
        void shouldReportQueryFailedWhenJsonInvalid() {
            // Given
            String json = "{";

            // When
            Optional<Query> query = queryMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(query).isNotPresent();
            assertThat(queryTracker.getFailedQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                    .containsExactly(TrackedQuery.builder()
                            .queryId("invalid-query-id")
                            .lastKnownState(QueryState.FAILED)
                            .errorMessage("java.io.EOFException: End of input at line 1 column 2 path $.")
                            .build());
        }

        @Test
        void shouldReportQueryFailedWhenMissingMandatoryField() {
            // Given json is missing tableName
            String json = "{" +
                    "  \"queryId\": \"my-query\"," +
                    "  \"resultsPublisherConfig\": {}," +
                    "  \"type\": \"Query\"," +
                    "  \"keys\": [{" +
                    "    \"field1\": 10" +
                    "  }]" +
                    "}";

            // When
            Optional<Query> query = queryMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(query).isNotPresent();
            assertThat(queryTracker.getFailedQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                    .containsExactly(TrackedQuery.builder()
                            .queryId("my-query")
                            .lastKnownState(QueryState.FAILED)
                            .errorMessage("Query validation failed for query \"my-query\": " +
                                    "tableName field must be provided")
                            .build());
        }

        @Test
        void shouldReportQueryFailedWithInvalidQueryType() {
            // Given
            String json = "{" +
                    "  \"queryId\": \"my-query\"," +
                    "  \"type\": \"invalid-query-type\"," +
                    "  \"resultsPublisherConfig\": {}," +
                    "  \"tableName\": \"table-1\"," +
                    "  \"keys\": [{" +
                    "    \"field1\": 10" +
                    "  }]" +
                    "}";

            // When
            Optional<Query> query = queryMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(query).isNotPresent();
            assertThat(queryTracker.getFailedQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                    .containsExactly(TrackedQuery.builder()
                            .queryId("my-query")
                            .lastKnownState(QueryState.FAILED)
                            .errorMessage("Query validation failed for query \"my-query\": " +
                                    "Unknown query type \"invalid-query-type\"")
                            .build());
        }
    }

    @Nested
    @DisplayName("Failed to validate")
    class FailedToValidate {
        @Test
        void shouldReportQueryFailedWhenTableDoesNotExist() {
            // Given
            String json = "{" +
                    "  \"queryId\": \"my-query\"," +
                    "  \"type\": \"Query\"," +
                    "  \"resultsPublisherConfig\": {}," +
                    "  \"tableName\": \"not-a-table\"," +
                    "  \"regions\": [{" +
                    "    \"key\": {" +
                    "        \"min\": \"123\"," +
                    "        \"minInclusive\": true," +
                    "        \"max\": \"456\"," +
                    "        \"maxInclusive\": false" +
                    "      }" +
                    "  }]" +
                    "}";

            // When
            Optional<Query> query = queryMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(query).isNotPresent();
            assertThat(queryTracker.getFailedQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                    .containsExactly(TrackedQuery.builder()
                            .queryId("my-query")
                            .lastKnownState(QueryState.FAILED)
                            .errorMessage("Query validation failed for query \"my-query\": " +
                                    "Table could not be found with name: \"not-a-table\"")
                            .build());
        }

        @Test
        void shouldReportQueryFailedWhenKeyDoesNotExist() {
            // Given
            String json = "{" +
                    "  \"queryId\": \"my-query\"," +
                    "  \"type\": \"Query\"," +
                    "  \"resultsPublisherConfig\": {}," +
                    "  \"tableName\": \"table-1\"," +
                    "  \"regions\": [{" +
                    "    \"not-a-key\": {" +
                    "        \"min\": \"123\"," +
                    "        \"minInclusive\": true," +
                    "        \"max\": \"456\"," +
                    "        \"maxInclusive\": false" +
                    "      }" +
                    "  }]" +
                    "}";

            // When
            Optional<Query> query = queryMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(query).isNotPresent();
            assertThat(queryTracker.getFailedQueries())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                    .containsExactly(TrackedQuery.builder()
                            .queryId("my-query")
                            .lastKnownState(QueryState.FAILED)
                            .errorMessage("Query validation failed for query \"my-query\": " +
                                    "Key \"not-a-key\" was not a row key field in the table schema")
                            .build());
        }
    }

    @Test
    void shouldSuccessfullyDeserialiseAndValidateQuery() {
        // Given
        String json = "{" +
                "  \"queryId\": \"my-query\"," +
                "  \"type\": \"Query\"," +
                "  \"resultsPublisherConfig\": {}," +
                "  \"tableName\": \"table-1\"," +
                "  \"regions\": [{" +
                "    \"key\": {" +
                "        \"min\": \"123\"," +
                "        \"minInclusive\": true," +
                "        \"max\": \"456\"," +
                "        \"maxInclusive\": false" +
                "      }" +
                "  }]" +
                "}";

        // When
        Optional<Query> query = queryMessageHandler.deserialiseAndValidate(json);

        // Then
        assertThat(query).get().isEqualTo(new Query.Builder("table-1", "my-query",
                new Region(new Range(new Field("key", new LongType()), 123L, 456L))).build());
        assertThat(queryTracker.getFailedQueries()).isEmpty();
    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, instanceProperties.get(ID) + "-query-tracker");
        return instanceProperties;
    }

    private TableProperties createTable(String tableName) {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schemaWithKey("key"));
        return tableProperties;
    }
}
