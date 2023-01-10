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
package sleeper.query.model;

import com.google.gson.JsonParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.query.model.output.ResultsOutputConstants;
import sleeper.query.model.output.S3ResultsOutput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@RunWith(Parameterized.class)
public class QuerySerDeTest {
    private final boolean createSerDeFromTablePropertiesProvider;

    public QuerySerDeTest(boolean createSerDeFromTablePropertiesProvider) {
        this.createSerDeFromTablePropertiesProvider = createSerDeFromTablePropertiesProvider;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> alternateTestParameters() {
        return Arrays.asList(new Object[][]{
                {false}, // Uses map to create the QuerySerDe
                {true}   // Uses tablepropertiesprovider
        });
    }

    private QuerySerDe generateQuerySerDe(String tableName, Schema schema) {
        if (createSerDeFromTablePropertiesProvider) {
            TestPropertiesProvider testPropertiesProvider = new TestPropertiesProvider(tableName, schema);
            return new QuerySerDe(testPropertiesProvider);
        }
        return new QuerySerDe(Collections.singletonMap(tableName, schema));
    }

    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder()
            .rowKeyFields(field)
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();

    @Test
    public void shouldSerDeFromJSONStringMinInclusiveMaxExclusive() {
        // Given
        RangeFactory rangeFactory = new RangeFactory(schema);
        QuerySerDe querySerDe = generateQuerySerDe("my-table", schema);
        String serialisedQuery = "{\n" +
                "  \"queryId\": \"my-query\",\n" +
                "  \"requestedValueFields\": [\n" +
                "    \"value1\"\n" +
                "  ],\n" +
                "  \"tableName\": \"my-table\",\n" +
                "  \"resultsPublisherConfig\": {},\n" +
                "  \"type\": \"Query\",\n" +
                "  \"regions\": [\n" +
                "    {\n" +
                "      \"key\": {\n" +
                "        \"min\": 1,\n" +
                "        \"minInclusive\": true,\n" +
                "        \"max\": 2,\n" +
                "        \"maxInclusive\": false\n" +
                "      },\n" +
                "      \"stringsBase64Encoded\": true\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // When
        Query query = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(query).isNotInstanceOf(LeafPartitionQuery.class);
        assertThat(query.getQueryId()).isEqualTo("my-query");
        assertThat(query.getTableName()).isEqualTo("my-table");
        assertThat(query.getResultsPublisherConfig()).isEqualTo(new HashMap<>());
        Region expectedRegion = new Region(rangeFactory.createRange(field, 1, true, 2, false));
        assertThat(query.getRegions()).containsExactly(expectedRegion);
        assertThat(query.getRequestedValueFields()).isEqualTo(Collections.singletonList("value1"));
    }

    @Test
    public void shouldSerDeFromJSONStringMinInclusiveMaxInclusive() {
        // Given
        RangeFactory rangeFactory = new RangeFactory(schema);
        QuerySerDe querySerDe = generateQuerySerDe("my-table", schema);
        String serialisedQuery = "{\n" +
                "  \"queryId\": \"my-query\",\n" +
                "  \"tableName\": \"my-table\",\n" +
                "  \"resultsPublisherConfig\": {},\n" +
                "  \"type\": \"Query\",\n" +
                "  \"regions\": [\n" +
                "    {\n" +
                "      \"key\": {\n" +
                "        \"min\": 1,\n" +
                "        \"minInclusive\": true,\n" +
                "        \"max\": 2,\n" +
                "        \"maxInclusive\": true\n" +
                "      },\n" +
                "      \"stringsBase64Encoded\": true\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // When
        Query query = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(query).isNotInstanceOf(LeafPartitionQuery.class);
        assertThat(query.getQueryId()).isEqualTo("my-query");
        assertThat(query.getTableName()).isEqualTo("my-table");
        assertThat(query.getResultsPublisherConfig()).isEqualTo(new HashMap<>());
        Region expectedRegion = new Region(rangeFactory.createRange(field, 1, true, 2, true));
        assertThat(query.getRegions()).containsExactly(expectedRegion);
        assertThat(query.getRequestedValueFields()).isNull();
    }

    @Test
    public void shouldSerDeFromJSONStringMinExclusiveMaxInclusive() {
        // Given
        RangeFactory rangeFactory = new RangeFactory(schema);
        QuerySerDe querySerDe = generateQuerySerDe("my-table", schema);
        String serialisedQuery = "{\n" +
                "  \"queryId\": \"my-query\",\n" +
                "  \"tableName\": \"my-table\",\n" +
                "  \"resultsPublisherConfig\": {},\n" +
                "  \"type\": \"Query\",\n" +
                "  \"regions\": [\n" +
                "    {\n" +
                "      \"key\": {\n" +
                "        \"min\": 1,\n" +
                "        \"minInclusive\": false,\n" +
                "        \"max\": 2,\n" +
                "        \"maxInclusive\": true\n" +
                "      },\n" +
                "      \"stringsBase64Encoded\": true\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // When
        Query query = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(query).isNotInstanceOf(LeafPartitionQuery.class);
        assertThat(query.getQueryId()).isEqualTo("my-query");
        assertThat(query.getTableName()).isEqualTo("my-table");
        assertThat(query.getResultsPublisherConfig()).isEqualTo(new HashMap<>());
        Region expectedRegion = new Region(rangeFactory.createRange(field, 1, false, 2, true));
        assertThat(query.getRegions()).containsExactly(expectedRegion);
        assertThat(query.getRequestedValueFields()).isNull();
    }

    @Test
    public void shouldSerDeFromJSONStringMinExclusiveMaxExclusive() {
        // Given
        RangeFactory rangeFactory = new RangeFactory(schema);
        QuerySerDe querySerDe = generateQuerySerDe("my-table", schema);
        String serialisedQuery = "{\n" +
                "  \"queryId\": \"my-query\",\n" +
                "  \"tableName\": \"my-table\",\n" +
                "  \"resultsPublisherConfig\": {},\n" +
                "  \"type\": \"Query\",\n" +
                "  \"regions\": [\n" +
                "    {\n" +
                "      \"key\": {\n" +
                "        \"min\": 1,\n" +
                "        \"minInclusive\": false,\n" +
                "        \"max\": 2,\n" +
                "        \"maxInclusive\": false\n" +
                "      },\n" +
                "      \"stringsBase64Encoded\": true\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // When
        Query query = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(query).isNotInstanceOf(LeafPartitionQuery.class);
        assertThat(query.getQueryId()).isEqualTo("my-query");
        assertThat(query.getTableName()).isEqualTo("my-table");
        assertThat(query.getResultsPublisherConfig()).isEqualTo(new HashMap<>());
        Region expectedRegion = new Region(rangeFactory.createRange(field, 1, false, 2, false));
        assertThat(query.getRegions()).containsExactly(expectedRegion);
        assertThat(query.getRequestedValueFields()).isNull();
    }

    @Test
    public void shouldSerDeFromJSONStringMinExclusiveNullMax() {
        // Given
        RangeFactory rangeFactory = new RangeFactory(schema);
        TestPropertiesProvider testPropertiesProvider = new TestPropertiesProvider("my-table", schema);
        QuerySerDe querySerDe = new QuerySerDe(testPropertiesProvider);
        String serialisedQuery = "{\n" +
                "  \"queryId\": \"my-query\",\n" +
                "  \"tableName\": \"my-table\",\n" +
                "  \"resultsPublisherConfig\": {},\n" +
                "  \"type\": \"Query\",\n" +
                "  \"regions\": [\n" +
                "    {\n" +
                "      \"key\": {\n" +
                "        \"min\": 1,\n" +
                "        \"minInclusive\": false,\n" +
                "        \"max\": null,\n" +
                "        \"maxInclusive\": false\n" +
                "      },\n" +
                "      \"stringsBase64Encoded\": true\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // When
        Query query = querySerDe.fromJson(serialisedQuery);

        // Then
        assertThat(query).isNotInstanceOf(LeafPartitionQuery.class);
        assertThat(query.getQueryId()).isEqualTo("my-query");
        assertThat(query.getTableName()).isEqualTo("my-table");
        assertThat(query.getResultsPublisherConfig()).isEqualTo(new HashMap<>());
        Region expectedRegion = new Region(rangeFactory.createRange(field, 1, false, null, false));
        assertThat(query.getRegions()).containsExactly(expectedRegion);
        assertThat(query.getRequestedValueFields()).isNull();
    }

    @Test
    public void shouldSerDeWhenSchemaHasIntKey() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        TestPropertiesProvider testPropertiesProvider = new TestPropertiesProvider(tableName, schema);
        Region region = new Region(rangeFactory.createExactRange(field, 1));
        Query query = new Query.Builder(tableName, "id", region).build();
        QuerySerDe querySerDe = new QuerySerDe(testPropertiesProvider);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query, true));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeWhenSchemaHasLongKey() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createExactRange(field, 1L));
        Query query = new Query.Builder(tableName, "id", region).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeWhenSchemaHasStringKey() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createExactRange(field, "1"));
        Query query = new Query.Builder(tableName, "id", region).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeWhenSchemaHasByteArrayKey() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createExactRange(field, new byte[]{0, 1, 2}));
        Query query = new Query.Builder(tableName, "id", region).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeMultipleByteArrayRegions() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region1 = new Region(rangeFactory.createExactRange(field, new byte[]{0, 1, 2}));
        Region region2 = new Region(rangeFactory.createExactRange(field, new byte[]{3, 4}));
        Query query = new Query.Builder(tableName, "id", Arrays.asList(region1, region2)).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeByteArrayRegionWithPublisherConfig() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region1 = new Region(rangeFactory.createExactRange(field, new byte[]{0, 1, 2}));
        Region region2 = new Region(rangeFactory.createExactRange(field, new byte[]{3, 4}));
        Map<String, String> publisherConfig = new HashMap<>();
        publisherConfig.put(ResultsOutputConstants.DESTINATION, "s3");
        publisherConfig.put(S3ResultsOutput.S3_BUCKET, "aBucket");
        Query query = new Query.Builder(tableName, "id", Arrays.asList(region1, region2))
                .setResultsPublisherConfig(publisherConfig)
                .build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeIntKeyQueryDifferentIncludeExcludes() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region1 = new Region(rangeFactory.createRange(field, 1, true, 2, true));
        Region region2 = new Region(rangeFactory.createRange(field, 3, true, 4, false));
        Region region3 = new Region(rangeFactory.createRange(field, 5, false, 6, true));
        Region region4 = new Region(rangeFactory.createRange(field, 7, false, 8, false));
        Query query = new Query.Builder(tableName, "id", Arrays.asList(region1, region2, region3, region4)).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeLongKeyQueryDifferentIncludeExcludes() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region1 = new Region(rangeFactory.createRange(field, 1L, true, 2L, true));
        Region region2 = new Region(rangeFactory.createRange(field, 3L, true, 4L, false));
        Region region3 = new Region(rangeFactory.createRange(field, 5L, false, 6L, true));
        Region region4 = new Region(rangeFactory.createRange(field, 7L, false, 8L, false));
        Query query = new Query.Builder(tableName, "id", Arrays.asList(region1, region2, region3, region4)).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeStringKeyQueryDifferentIncludeExcludes() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region1 = new Region(rangeFactory.createRange(field, "1", true, "2", true));
        Region region2 = new Region(rangeFactory.createRange(field, "3", true, "4", false));
        Region region3 = new Region(rangeFactory.createRange(field, "5", false, "6", true));
        Region region4 = new Region(rangeFactory.createRange(field, "7", false, "8", false));
        Query query = new Query.Builder(tableName, "id", Arrays.asList(region1, region2, region3, region4)).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeStringKeyQueryWithNull() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createRange(field, "A", true, null, false));
        Query query = new Query.Builder(tableName, "id", region).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeByteArrayKeyQueryDifferentIncludeExcludes() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region1 = new Region(rangeFactory.createRange(field, new byte[]{0, 1, 2}, true, new byte[]{4}, true));
        Region region2 = new Region(rangeFactory.createRange(field, new byte[]{0, 1, 2}, true, new byte[]{4}, false));
        Region region3 = new Region(rangeFactory.createRange(field, new byte[]{0, 1, 2}, false, new byte[]{4}, true));
        Region region4 = new Region(rangeFactory.createRange(field, new byte[]{0, 1, 2}, false, new byte[]{4}, false));
        Query query = new Query.Builder(tableName, "id", Arrays.asList(region1, region2, region3, region4)).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeByteArrayQueryWithNull() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createRange(field, new byte[]{1, 2}, true, null, false));
        Query query = new Query.Builder(tableName, "id", region).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeByteArrayKeyLeafPartitionQuery() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        files.add("file3");
        Region region = new Region(rangeFactory.createRange(field, new byte[]{0, 1, 2}, new byte[]{4}));
        Region partitionRegion = new Region(rangeFactory.createRange(field, new byte[]{0}, new byte[]{100}));
        LeafPartitionQuery query = new LeafPartitionQuery
                .Builder(tableName, "id", "subid", region, "leaf", partitionRegion, files)
                .build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        System.out.println(querySerDe.toJson(query));

        LeafPartitionQuery deserialisedQuery = (LeafPartitionQuery) querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeMultipleByteArrayKeyLeafPartitionQuery() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        files.add("file3");
        Region region1 = new Region(rangeFactory.createRange(field, new byte[]{0, 1, 2}, true, new byte[]{4}, true));
        Region region2 = new Region(rangeFactory.createRange(field, new byte[]{10}, true, new byte[]{20}, true));
        Region partitionRegion = new Region(rangeFactory.createRange(field, new byte[]{0}, new byte[]{100}));
        LeafPartitionQuery query = new LeafPartitionQuery
                .Builder(tableName, "id", "subid", Arrays.asList(region1, region2), "leaf", partitionRegion, files)
                .build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        LeafPartitionQuery deserialisedQuery = (LeafPartitionQuery) querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldSerDeIntKeyWithIterator() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createRange(field, 1, true, 5, true));
        Query query = new Query.Builder(tableName, "id", region)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When
        Query deserialisedQuery = querySerDe.fromJson(querySerDe.toJson(query));

        // Then
        assertThat(deserialisedQuery).isEqualTo(query);
    }

    @Test
    public void shouldThrowExceptionWithNullTableName() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        String tableName = UUID.randomUUID().toString();
        Region region = new Region(rangeFactory.createExactRange(field, new byte[]{0, 1, 2}));
        Query query = new Query.Builder(null, "id", region).build();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        // When & Then
        if (createSerDeFromTablePropertiesProvider) {
            // When the QuerySerDe is created from a TablePropertiesProvider,
            // the TablePropertiesProvider will return TableProperties for a table with a null name.
            String json = querySerDe.toJson(query);
            assertThatThrownBy(() -> querySerDe.fromJson(json))
                    .isInstanceOf(JsonParseException.class)
                    .hasMessage("tableName field must be provided");
        } else {
            // When the QuerySerDe is created from a Map, a NullPointerException is thrown
            // when retrieving a table with a null name.
            assertThatThrownBy(() -> querySerDe.toJson(query))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    public void shouldThrowExceptionNoTableName() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new ByteArrayType())).build();
        String tableName = UUID.randomUUID().toString();
        QuerySerDe querySerDe = generateQuerySerDe(tableName, schema);

        String queryJson = "{\n" +
                "  \"queryId\": \"id\",\n" +
                "  \"type\": \"Query\",\n" +
                "  \"keys\": [\n" +
                "  \t{\"field1\": 10}\n" +
                "  ]\n" +
                "}\n";

        // When & Then
        assertThatThrownBy(() -> querySerDe.fromJson(queryJson))
                .isInstanceOf(JsonParseException.class)
                .hasMessage("tableName field must be provided");
    }

    private static class TestPropertiesProvider extends TablePropertiesProvider {
        private final TableProperties tableProperties;

        TestPropertiesProvider(final String tableName, final Schema schema) {
            super(null, null);
            this.tableProperties = new TableProperties(new InstanceProperties());
            tableProperties.set(TABLE_NAME, tableName);
            tableProperties.setSchema(schema);
        }

        @Override
        public TableProperties getTableProperties(final String tableName) {
            return tableProperties;
        }
    }
}
