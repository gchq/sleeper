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
package sleeper.systemtest.drivers.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.NoInstanceConnectedException;
import sleeper.systemtest.dsl.instance.NoTableChosenException;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;

@LocalStackDslTest
public class SleeperInstanceTablesLocalIT {
    @TempDir
    private Path tempDir;

    @Nested
    @DisplayName("Define named tables")
    class DefineNamedTables {
        @BeforeEach
        void setUp(SleeperSystemTest sleeper) {
            sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
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
            sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
        }

        @Test
        void shouldQueryRecordsForNamedTables(SleeperSystemTest sleeper) {
            // Given
            sleeper.tables().create(List.of("A", "B"), schemaWithKey("key", new LongType()));
            sleeper.table("A").ingest().direct(tempDir).numberedRecords(LongStream.of(1, 2));
            sleeper.table("B").ingest().direct(tempDir).numberedRecords(LongStream.of(3, 4));

            // When / Then
            Map<String, List<Record>> expectedRecords = Map.of(
                    "A", List.of(
                            new Record(Map.of("key", 1L)),
                            new Record(Map.of("key", 2L))),
                    "B", List.of(
                            new Record(Map.of("key", 3L)),
                            new Record(Map.of("key", 4L))));
            assertThat(sleeper.directQuery().allRecordsByTable()).isEqualTo(expectedRecords);
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
            sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
            var ingest = sleeper.ingest().direct(null);

            // When / Then
            assertThatThrownBy(() -> ingest.numberedRecords(LongStream.of(1, 2, 3)))
                    .isInstanceOf(NoTableChosenException.class);
        }
    }
}
