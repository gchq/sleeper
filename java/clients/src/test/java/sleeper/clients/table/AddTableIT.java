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

package sleeper.clients.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class AddTableIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
    }

    @Test
    void shouldAddTableWithNoPredefinedSplitPoints() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

        // When
        addTable(tableProperties);

        // Then
        TableProperties foundProperties = propertiesStore.loadByName(tableProperties.get(TABLE_NAME));
        assertThat(foundProperties).isEqualTo(tableProperties);
        assertThat(foundProperties.get(TABLE_ID)).isNotEmpty();
        assertThat(stateStore(foundProperties).getAllPartitions())
                .containsExactlyElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .buildList());
    }

    @Test
    void shouldFailToAddTableIfTableAlreadyExists() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        addTable(tableProperties);

        // When / Then
        assertThatThrownBy(() -> addTable(tableProperties))
                .isInstanceOf(TableAlreadyExistsException.class);
    }

    @Test
    void shouldFailToAddTableIfNameNotSet() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.unset(TABLE_NAME);

        // When / Then
        assertThatThrownBy(() -> addTable(tableProperties))
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }

    @Test
    void shouldAddTableWithSplitPoints() throws Exception {
        // Given
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        Files.writeString(tempDir.resolve("splitpoints.txt"), "100");
        tableProperties.set(TableProperty.SPLIT_POINTS_FILE, tempDir.resolve("splitpoints.txt").toString());

        // When
        addTable(tableProperties);

        // Then
        assertThat(stateStore(tableProperties).getAllPartitions())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 100L)
                        .buildList());
    }

    private StateStore stateStore(TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private void addTable(TableProperties tableProperties) throws IOException {
        new AddTable(instanceProperties, tableProperties,
                S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient),
                StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                .run();
    }
}
