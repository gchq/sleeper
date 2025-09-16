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
package sleeper.query.core.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.output.ResultsOutput;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QuerySerDeTest {

    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder()
            .rowKeyFields(field)
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_NAME, "my-table");
        tableProperties.set(TABLE_ID, "my-table-id");
    }

    @Nested
    @DisplayName("Read and write all fields")
    class ReadAndWriteAllFields {

        QueryProcessingConfig processingConfigWithAllFieldsSet = QueryProcessingConfig.builder()
                .queryTimeIteratorClassName("TestIterator")
                .queryTimeIteratorConfig("config")
                .requestedValueFields(List.of("integer"))
                .resultsPublisherConfig(Map.of(ResultsOutput.DESTINATION, "results-target"))
                .statusReportDestinations(List.of(Map.of(ResultsOutput.DESTINATION, "status-report-target")))
                .build();

        @ParameterizedTest
        @MethodSource("serDeConstructors")
        void shouldSerDeParentQueryDirectlyFromSchema(QuerySerDeConstructor constructor) {
            // Given
            Query query = Query.builder()
                    .queryId("test-query")
                    .tableName("my-table")
                    .regions(List.of(regionWithOneRange(factory -> factory
                            .createRange("key", 10, 20))))
                    .processingConfig(processingConfigWithAllFieldsSet)
                    .build();

            // When
            QuerySerDe serDe = constructor.createSerDe(tableProperties);
            String json = serDe.toJson(query);
            Query found = serDe.fromJson(json);

            // Then
            assertThat(found).isEqualTo(query);
        }

        @ParameterizedTest
        @MethodSource("serDeConstructors")
        void shouldSerDeLeafQueryDirectlyFromSchema(QuerySerDeConstructor constructor) {
            // Given
            PartitionTree partitions = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100)
                    .buildTree();
            LeafPartitionQuery query = LeafPartitionQuery.builder()
                    .queryId("test-query")
                    .subQueryId("test-subquery")
                    .tableId("my-table-id")
                    .leafPartitionId("L")
                    .regions(List.of(regionWithOneRange(factory -> factory
                            .createRange("key", 10, 20))))
                    .partitionRegion(partitions.getPartition("L").getRegion())
                    .files(List.of("file-1", "file-2"))
                    .processingConfig(processingConfigWithAllFieldsSet)
                    .build();

            // When
            QuerySerDe serDe = constructor.createSerDe(tableProperties);
            String json = serDe.toJson(query);
            LeafPartitionQuery found = serDe.fromJsonOrLeafQuery(json).asLeafQuery();

            // Then
            assertThat(found).isEqualTo(query);
        }

        private static Stream<Arguments> serDeConstructors() {
            return Stream.of(
                    Arguments.of(Named.of("QuerySerDe from schema", serDeFromSchema())),
                    Arguments.of(Named.of("QuerySerDe from table properties provider", serDeFromPropertiesProvider())));
        }

        private static QuerySerDeConstructor serDeFromSchema() {
            return properties -> new QuerySerDe(properties.getSchema());
        }

        private static QuerySerDeConstructor serDeFromPropertiesProvider() {
            return properties -> new QuerySerDe(new FixedTablePropertiesProvider(properties));
        }

        public interface QuerySerDeConstructor {
            QuerySerDe createSerDe(TableProperties tableProperties);
        }
    }

    @Nested
    @DisplayName("Handle regions")
    class HandleRegions {

        @Test
        public void shouldSerDeMultipleByteArrayRegions() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new ByteArrayType()));
            Query query = Query.builder()
                    .tableName("my-table")
                    .queryId("id")
                    .regions(List.of(
                            regionWithOneRange(factory -> factory
                                    .createExactRange("key", new byte[]{0, 1, 2})),
                            regionWithOneRange(factory -> factory
                                    .createExactRange("key", new byte[]{3, 4}))))
                    .build();

            // When
            QuerySerDe querySerDe = createSerDe();
            String json = querySerDe.toJson(query);
            Query deserialisedQuery = querySerDe.fromJsonOrLeafQuery(json).asParentQuery();

            // Then
            assertThat(deserialisedQuery).isEqualTo(query);
        }

        @Test
        void shouldSerDeLeafPartitionQueryWithDifferentRegionsFromParent() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("key", new LongType()));
            PartitionTree partitions = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 1000L)
                    .buildTree();
            Region region1 = regionWithOneRange(factory -> factory.createRange("key", -100L, true, -10L, true));
            Region region2 = regionWithOneRange(factory -> factory.createRange("key", 10L, true, 100L, true));
            Query parentQuery = Query.builder()
                    .tableName("my-table")
                    .queryId("id")
                    .regions(List.of(region1, region2))
                    .build();
            LeafPartitionQuery query = LeafPartitionQuery.builder()
                    .parentQuery(parentQuery).regions(List.of(region2))
                    .tableId(tableProperties.get(TABLE_ID))
                    .subQueryId("subid")
                    .leafPartitionId("L")
                    .partitionRegion(partitions.getPartition("L").getRegion())
                    .files(List.of("file1", "file2", "file3"))
                    .build();

            // When
            QuerySerDe querySerDe = createSerDe();
            String json = querySerDe.toJson(query);
            LeafPartitionQuery deserialisedQuery = querySerDe.fromJsonOrLeafQuery(json).asLeafQuery();

            // Then
            assertThat(deserialisedQuery).isEqualTo(query);
        }
    }

    @Nested
    @DisplayName("Validation")
    class Validation {

        @Test
        public void shouldThrowExceptionWithNoTableName() {
            // Given
            String queryJson = "{" +
                    "  \"queryId\": \"id\"," +
                    "  \"type\": \"Query\"," +
                    "  \"regions\": []" +
                    "}";

            // When / Then
            QuerySerDe querySerDe = createSerDe();
            assertThatThrownBy(() -> querySerDe.fromJsonOrLeafQuery(queryJson))
                    .isInstanceOf(QueryValidationException.class)
                    .hasMessage("Query validation failed for query \"id\": tableName field must be provided");
        }

        @Test
        public void shouldThrowExceptionWithNoQueryId() {
            // Given
            tableProperties.set(TABLE_NAME, "test-table");

            String queryJson = "{" +
                    "  \"type\": \"Query\"," +
                    "  \"tableName\": \"test-table\"," +
                    "  \"regions\": []" +
                    "}";

            // When / Then
            QuerySerDe querySerDe = createSerDe();
            assertThatThrownBy(() -> querySerDe.fromJsonOrLeafQuery(queryJson))
                    .isInstanceOf(QueryValidationException.class)
                    .hasMessage("Query validation failed: queryId field must be provided");
        }

        @Test
        public void shouldThrowExceptionWithNoQueryType() {
            // Given
            tableProperties.set(TABLE_NAME, "test-table");

            String queryJson = "{" +
                    "  \"queryId\": \"id\"," +
                    "  \"tableName\": \"test-table\"," +
                    "  \"regions\": []" +
                    "}\n";

            // When / Then
            QuerySerDe querySerDe = createSerDe();
            assertThatThrownBy(() -> querySerDe.fromJsonOrLeafQuery(queryJson))
                    .isInstanceOf(QueryValidationException.class)
                    .hasMessage("Query validation failed for query \"id\": " +
                            "type field must be provided");
        }

        @Test
        public void shouldThrowExceptionWithInvalidQueryType() {
            // Given
            tableProperties.setSchema(createSchemaWithKey("field1", new LongType()));

            String queryJson = "{" +
                    "  \"queryId\": \"id\"," +
                    "  \"type\": \"invalid-query-type\"," +
                    "  \"tableName\": \"test-table\"," +
                    "  \"regions\": []" +
                    "}\n";

            // When / Then
            assertThatThrownBy(() -> createSerDe().fromJsonOrLeafQuery(queryJson))
                    .isInstanceOf(QueryValidationException.class)
                    .hasMessage("Query validation failed for query \"id\": " +
                            "Unknown query type \"invalid-query-type\"");
        }
    }

    private QuerySerDe createSerDe() {
        return new QuerySerDe(new FixedTablePropertiesProvider(tableProperties));
    }

    private Region regionWithOneRange(Function<RangeFactory, Range> createRange) {
        return new Region(createRange.apply(new RangeFactory(tableProperties.getSchema())));
    }
}
