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

package sleeper.systemtest.dsl.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.SleeperPropertiesInvalidException;
import sleeper.configuration.properties.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.systemtest.dsl.instance.SystemTestInstanceConfiguration.usingSystemTestDefaults;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.createDslInstanceProperties;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.createDslTableProperties;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.withDefaultProperties;

@InMemoryDslTest
public class SleeperInstanceTablesTest {

    @Nested
    @DisplayName("Define named tables")
    class DefineNamedTables {
        @BeforeEach
        void setUp(SleeperSystemTest sleeper) {
            sleeper.connectToInstanceNoTables(withDefaultProperties("main"));
        }

        @Test
        void shouldCreateTwoTablesWithDifferentPartitionsAndSchemas(SleeperSystemTest sleeper) {
            // Given
            Schema schemaA = schemaWithKey("keyA", new StringType());
            Schema schemaB = schemaWithKey("keyA", new LongType());
            PartitionTree partitionsA = new PartitionsBuilder(schemaA)
                    .rootFirst("A-root")
                    .splitToNewChildren("A-root", "AL", "AR", "aaa")
                    .buildTree();
            PartitionTree partitionsB = new PartitionsBuilder(schemaB)
                    .rootFirst("B-root")
                    .splitToNewChildren("B-root", "BL", "BR", 50L)
                    .buildTree();
            sleeper.tables()
                    .create("A", schemaA)
                    .create("B", schemaB);

            // When
            sleeper.table("A").partitioning().setPartitions(partitionsA);
            sleeper.table("B").partitioning().setPartitions(partitionsB);

            // Then
            assertThat(sleeper.partitioning().treeByTable())
                    .isEqualTo(Map.of("A", partitionsA, "B", partitionsB));
            assertThat(sleeper.tables().list()).hasSize(2);
        }

        @Test
        void shouldSetPartitionsForMultipleTables(SleeperSystemTest sleeper) {
            // Given
            Schema schema = schemaWithKey("key", new StringType());
            sleeper.tables().create(List.of("A", "B"), schema);
            PartitionTree partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "aaa")
                    .buildTree();

            // When
            sleeper.tables().forEach(() -> sleeper.partitioning().setPartitions(partitions));

            // Then
            assertThat(sleeper.partitioning().treeByTable())
                    .isEqualTo(Map.of("A", partitions, "B", partitions));
            assertThat(sleeper.tables().list()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Load data for all tables at once")
    class InspectAllTables {
        @BeforeEach
        void setUp(SleeperSystemTest sleeper) {
            sleeper.connectToInstanceNoTables(withDefaultProperties("main"));
        }

        @Test
        void shouldQueryRecordsForNamedTables(SleeperSystemTest sleeper) {
            // Given
            sleeper.tables().create(List.of("A", "B"), schemaWithKey("key", new LongType()));
            sleeper.table("A").ingest().direct(null).numberedRecords(LongStream.of(1, 2));
            sleeper.table("B").ingest().direct(null).numberedRecords(LongStream.of(3, 4));

            // When / Then
            Map<String, List<Record>> expectedRecords = Map.of(
                    "A", List.of(
                            new Record(Map.of("key", 1L)),
                            new Record(Map.of("key", 2L))),
                    "B", List.of(
                            new Record(Map.of("key", 3L)),
                            new Record(Map.of("key", 4L))));
            assertThat(sleeper.directQuery().allRecordsByTable()).isEqualTo(expectedRecords);
            assertThat(sleeper.query().byQueue().allRecordsByTable()).isEqualTo(expectedRecords);
        }

        @Test
        void shouldQueryNoRecordsForNamedTables(SleeperSystemTest sleeper) {
            // Given
            sleeper.tables().create(List.of("A", "B"), schemaWithKey("key", new LongType()));

            // When / Then
            Map<String, List<Record>> expectedRecords = Map.of(
                    "A", List.of(),
                    "B", List.of());
            assertThat(sleeper.directQuery().allRecordsByTable()).isEqualTo(expectedRecords);
            assertThat(sleeper.query().byQueue().allRecordsByTable()).isEqualTo(expectedRecords);
        }

        @Test
        void shouldNotIncludeTablesNotManagedByDsl(SleeperSystemTest sleeper, SystemTestDrivers drivers, SystemTestContext context) {
            // Given
            InstanceProperties instanceProperties = context.instance().getInstanceProperties();
            TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));

            // When
            drivers.tables(context.parameters()).addTable(instanceProperties, tableProperties);

            // Then
            assertThat(sleeper.tables().list()).isEmpty();
            assertThat(sleeper.directQuery().allRecordsByTable()).isEmpty();
            assertThat(sleeper.query().byQueue().allRecordsByTable()).isEmpty();
        }

        @Test
        void shouldIncludeUnnamedTables(SleeperSystemTest sleeper) {
            // When
            sleeper.tables().createMany(2, schemaWithKey("key"));

            // Then
            assertThat(sleeper.tables().list()).hasSize(2);
            assertThat(sleeper.directQuery().allRecordsByTable()).hasSize(2);
            assertThat(sleeper.query().byQueue().allRecordsByTable()).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Derive actual table name from name specified by test")
    class DeriveTableName {

        @Test
        void shouldGenerateNameForTableDefinedInTest(SleeperSystemTest sleeper) {
            // Given
            sleeper.connectToInstanceNoTables(withDefaultProperties("main"));

            // When
            sleeper.tables().create("A", DEFAULT_SCHEMA);

            // Then
            assertThat(sleeper.table("A").tableProperties().get(TABLE_NAME))
                    .startsWith("A-")
                    .hasSize(38);
        }

        @Test
        void shouldGenerateNameForPredefinedTable(SleeperSystemTest sleeper) {
            // When
            sleeper.connectToInstance(usingSystemTestDefaults("prdftbl", () -> {
                InstanceProperties instanceProperties = createDslInstanceProperties();
                TableProperties tableProperties = createDslTableProperties(instanceProperties);
                tableProperties.set(TABLE_NAME, "predefined-test-table");
                return new DeployInstanceConfiguration(instanceProperties, tableProperties);
            }));

            // Then
            assertThat(sleeper.tableProperties().get(TABLE_NAME))
                    .startsWith("predefined-test-table-")
                    .hasSize(58);
        }

        @Test
        void shouldRefusePredefinedTableWithNoName(SleeperSystemTest sleeper) {
            // Given
            SystemTestInstanceConfiguration configuration = usingSystemTestDefaults("nonmtbl", () -> {
                InstanceProperties instanceProperties = createDslInstanceProperties();
                TableProperties tableProperties = createDslTableProperties(instanceProperties);
                tableProperties.unset(TABLE_NAME);
                return new DeployInstanceConfiguration(instanceProperties, tableProperties);
            });

            // When / Then
            assertThatThrownBy(() -> sleeper.connectToInstance(configuration))
                    .isInstanceOf(SleeperPropertiesInvalidException.class);
        }
    }

    @Nested
    @DisplayName("Fail when no instance/table chosen")
    class FailWithNoInstanceOrTable {

        @Test
        void shouldFailToIngestWhenNoInstanceConnected(SleeperSystemTest sleeper) {
            // When / Then
            assertThatThrownBy(() -> sleeper.ingest())
                    .isInstanceOf(NoInstanceConnectedException.class);
        }

        @Test
        void shouldFailToIngestWhenNoTableChosen(SleeperSystemTest sleeper) {
            // Given
            sleeper.connectToInstanceNoTables(withDefaultProperties("main"));
            var ingest = sleeper.ingest().direct(null);

            // When / Then
            assertThatThrownBy(() -> ingest.numberedRecords(LongStream.of(1, 2, 3)))
                    .isInstanceOf(NoTableChosenException.class);
        }
    }
}
