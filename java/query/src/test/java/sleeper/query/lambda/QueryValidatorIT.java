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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.CommonTestConstants;
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
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class QueryValidatorIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, DYNAMO_PORT, AmazonDynamoDBClientBuilder.standard());
    }

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

    @BeforeEach
    void setUp() {
        new DynamoDBQueryTrackerCreator(instanceProperties, dynamoDBClient).create();
    }

    @Test
    void shouldReportValidationFailureWhenJsonInvalid() {
        // Given
        String json = "{";

        // When
        QueryValidator queryValidator = new QueryValidator(null, queryTracker, () -> "invalid-query-id");
        Optional<Query> query = queryValidator.deserialiseAndValidate(json);

        // Then
        assertThat(query).isNotPresent();
        assertThat(queryTracker.getFailedQueries())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastUpdateTime", "expiryDate")
                .containsExactly(TrackedQuery.builder()
                        .queryId("invalid-query-id")
                        .lastKnownState(QueryState.FAILED)
                        .build());

    }

    private static InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, instanceProperties.get(ID) + "-query-tracker");
        return instanceProperties;
    }
}
