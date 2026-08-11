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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.athena.TestUtils;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.athena.TestUtils.createConstraints;
import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.MAX_ROW_KEY_PREFIX;
import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.MIN_ROW_KEY_PREFIX;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class DataFusionRecordHandlerIT extends RecordHandlerITBase {

    @Test
    public void shouldReadAllRowsInPartitionWhenNoPredicates() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, "F", "G", "U");

        // When
        RecordResponse rawResponse = readRecords(tableProperties, createArrowSchema(), new HashMap<>());

        // Then
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isEqualTo(12 * 28);
        assertRecordContained(response.getRecords(), 0, "F", 1, 1);
    }

    @Test
    public void shouldPushRowKeyExactMatchDownAsRegion() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, "F", "G", "U");
        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("key2", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(2).build());
        predicates.put("key3", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(28).build());

        // When
        RecordResponse rawResponse = readRecords(tableProperties, createArrowSchema(), predicates);

        // Then
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isOne();
        assertRecordContained(response.getRecords(), 0, "F", 2, 28);
    }

    @Test
    public void shouldPushRowKeyRangeDownAsRegion() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, "F", "G", "U");
        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("key1", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                "F", true, "G", false)));
        predicates.put("key2", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, false, 6, true)));

        // When
        RecordResponse rawResponse = readRecords(tableProperties, createArrowSchema(), predicates);

        // Then all 28 key3 values for key2 == 6 in the F partition
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isEqualTo(28);
        assertRecordContained(response.getRecords(), 0, "F", 6, 1);
        assertRecordContained(response.getRecords(), 27, "F", 6, 28);
    }

    @Test
    public void shouldPushValueFieldPredicateDownAsSql() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, "F", "G", "U");
        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("str", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(), true, false)
                .add("F-2-28").build());

        // When
        RecordResponse rawResponse = readRecords(tableProperties, createArrowSchema(), predicates);

        // Then
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isOne();
        assertRecordContained(response.getRecords(), 0, "F", 2, 28);
    }

    @Test
    public void shouldReadOnlyProjectedValueFields() throws Exception {
        // Given a projection of only some of the columns
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, "F", "G", "U");
        org.apache.arrow.vector.types.pojo.Schema projection = new SchemaBuilder()
                .addStringField("key1")
                .addIntField("key2")
                .addIntField("key3")
                .addBigIntField("count")
                .build();
        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("key2", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(2).build());
        predicates.put("key3", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(28).build());

        // When
        RecordResponse rawResponse = readRecords(tableProperties, projection, predicates);

        // Then only the projected columns are returned
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isOne();
        Block records = response.getRecords();
        assertThat(records.getSchema().getFields()).extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                .containsExactly("key1", "key2", "key3", "count");
        assertFieldContainedValue(records, 0, "key1", new Text("F"));
        assertFieldContainedValue(records, 0, "key2", 2);
        assertFieldContainedValue(records, 0, "key3", 28);
        assertFieldContainedValue(records, 0, "count", 2L * 28);
    }

    @Test
    public void shouldReturnNoRowsWhenPartitionHasNoFiles() throws Exception {
        // Given an empty table
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createEmptyTable(instanceProperties, "F", "G", "U");

        // When
        RecordResponse rawResponse = readRecords(tableProperties, List.of(), createArrowSchema(), new HashMap<>());

        // Then
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isZero();
    }

    private RecordResponse readRecords(
            TableProperties tableProperties, org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            Map<String, ValueSet> predicates) throws Exception {
        return readRecords(tableProperties, partitionFilesForKey1(tableProperties, "F"), arrowSchema, predicates);
    }

    private RecordResponse readRecords(
            TableProperties tableProperties, List<String> partitionFiles,
            org.apache.arrow.vector.types.pojo.Schema arrowSchema, Map<String, ValueSet> predicates) throws Exception {
        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();
        return handler().doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                arrowSchema,
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, new Gson().toJson(partitionFiles))
                        .add(MIN_ROW_KEY_PREFIX + "-key1", "F")
                        .add(MAX_ROW_KEY_PREFIX + "-key1", "G")
                        .add(MIN_ROW_KEY_PREFIX + "-key2", MIN_VALUE)
                        .add(MIN_ROW_KEY_PREFIX + "-key3", MIN_VALUE)
                        .build(),
                createConstraints(predicates),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE));
    }

    private List<String> partitionFilesForKey1(TableProperties tableProperties, String key1) {
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        return stateStore.getLeafPartitions().stream()
                .filter(p -> p.getRegion().getRange("key1").getMin().equals(key1))
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    private DataFusionRecordHandler handler() {
        return new DataFusionRecordHandler(
                s3Client, dynamoClient,
                getInstanceProperties().get(CONFIG_BUCKET),
                mock(SecretsManagerClient.class), mock(AthenaClient.class));
    }

    private void assertRecordContained(Block records, int position, String key1, int key2, int key3) {
        assertFieldContainedValue(records, position, "key1", new Text(key1));
        assertFieldContainedValue(records, position, "key2", key2);
        assertFieldContainedValue(records, position, "key3", key3);
        long timestamp = (long) key2 * 100 + key3;
        assertFieldContainedValue(records, position, "timestamp", timestamp);
        assertFieldContainedValue(records, position, "count", (long) key2 * key3);
        assertFieldContainedValue(records, position, "str", new Text(key1 + "-" + key2 + "-" + key3));
        assertFieldContainedValue(records, position, "list", Lists.newArrayList(new Text("listValue")));
        JsonStringHashMap<String, Object> mapEntry = new JsonStringHashMap<>();
        mapEntry.put("key", new Text("mapKey"));
        mapEntry.put("value", new Text("mapValue"));
        assertFieldContainedValue(records, position, "map", Lists.newArrayList(mapEntry));
    }
}
