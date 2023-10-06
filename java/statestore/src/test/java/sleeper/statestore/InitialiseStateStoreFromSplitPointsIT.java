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

package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.dynamodb.tools.DynamoDBContainer;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class InitialiseStateStoreFromSplitPointsIT {
    @Container
    public static DynamoDBContainer dynamoDb = new DynamoDBContainer();
    private static AmazonDynamoDB dynamoDBClient;

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @TempDir
    public Path tempDir;

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, dynamoDb.getDynamoPort(), AmazonDynamoDBClientBuilder.standard());
    }

    @Test
    void shouldInitialiseStateStoreFromSplitPoints() throws Exception {
        // Given
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();

        // When
        new InitialiseStateStoreFromSplitPoints(dynamoDBClient, instanceProperties, tableProperties, List.of(123L)).run();

        // Then
        StateStore stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        assertThat(stateStore.getAllPartitions())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                .containsExactlyInAnyOrderElementsOf(
                        new PartitionsBuilder(schema)
                                .rootFirst("root")
                                .splitToNewChildren("root", "left", "right", 123L)
                                .buildList());
    }

    @Test
    void shouldInitialiseStateStoreWithRootLeafPartitionIfSplitPointsNotProvided() throws Exception {
        // Given
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDBClient).create();

        // When
        new InitialiseStateStoreFromSplitPoints(dynamoDBClient, instanceProperties, tableProperties, null).run();

        // Then
        StateStore stateStore = new DynamoDBStateStore(instanceProperties, tableProperties, dynamoDBClient);
        assertThat(stateStore.getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(
                        new PartitionsBuilder(schema)
                                .singlePartition("root")
                                .buildList());
    }
}
