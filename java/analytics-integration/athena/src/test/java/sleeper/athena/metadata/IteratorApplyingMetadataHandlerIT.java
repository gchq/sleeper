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
package sleeper.athena.metadata;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.athena.TestUtils;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.splitter.core.split.SplitPartition;
import sleeper.statestore.StateStoreFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static sleeper.athena.TestUtils.createConstraints;
import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.MAX_ROW_KEY_PREFIX;
import static sleeper.athena.metadata.IteratorApplyingMetadataHandler.MIN_ROW_KEY_PREFIX;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class IteratorApplyingMetadataHandlerIT extends MetadataHandlerITBase {

    @Test
    public void shouldAddMinAndMaxRowKeysToTheRowsForEachLeafPartition() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        IteratorApplyingMetadataHandler sleeperMetadataHandler = handler(instance);
        StateStore stateStore = stateStore(instance, table);
        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));
        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(new HashMap<>()),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());
        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                request);

        // Then
        List<Partition> leafPartitions = stateStore.getLeafPartitions();
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(4);
        for (int i = 0; i < leafPartitions.size(); i++) {
            Partition partition = leafPartitions.get(i);
            for (Field field : SCHEMA.getRowKeyFields()) {
                FieldReader minReader = partitions.getFieldReader(MIN_ROW_KEY_PREFIX + "-" + field.getName());
                minReader.setPosition(i);
                assertThat(readRowKeyValue(minReader, field))
                        .isEqualTo(partition.getRegion().getRange(field.getName()).getMin());

                FieldReader maxReader = partitions.getFieldReader(MAX_ROW_KEY_PREFIX + "-" + field.getName());
                maxReader.setPosition(i);
                assertThat(readRowKeyValue(maxReader, field))
                        .isEqualTo(partition.getRegion().getRange(field.getName()).getMax());
            }
        }
    }

    @Test
    public void shouldPassOnTheListOfFilesAndRowKeysWhenCallingGetSplits() throws Exception {
        // Given
        InstanceProperties instance = createInstance();

        // When
        IteratorApplyingMetadataHandler sleeperMetadataHandler = handler(instance);

        GetSplitsResponse getSplitsResponse;
        try (BlockAllocator blockAllocator = new BlockAllocatorImpl()) {
            Block partitions = blockAllocator.createBlock(SchemaBuilder.newBuilder()
                    .addIntField("_MinRowKey-key1")
                    .addStringField("_MinRowKey-key2")
                    .addIntField("_MaxRowKey-key1")
                    .addStringField("_MaxRowKey-key2")
                    .addStringField(RELEVANT_FILES_FIELD)
                    .build());

            addPartition(partitions, 0, 25);
            addPartition(partitions, 1, 26);
            addPartition(partitions, 2, 27);

            getSplitsResponse = sleeperMetadataHandler.doGetSplits(blockAllocator,
                    new GetSplitsRequest(TestUtils.createIdentity(), "abc", "def", new TableName("myDB", "myTable"),
                            partitions, new ArrayList<>(), createConstraints(new HashMap<>()), "unused"));
        }

        // Then
        Set<Split> splits = getSplitsResponse.getSplits();
        assertThat(splits).hasSize(3);

        validateSplit(splits, 25);
        validateSplit(splits, 26);
        validateSplit(splits, 27);
    }

    @Test
    public void shouldIncludePartitionsWhenItHasBeenSplitBySystem() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);
        IteratorApplyingMetadataHandler sleeperMetadataHandler = handler(instance);
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        StateStore stateStore = stateStore(instance, table);
        Partition partitionForKey1 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("key1").getMin().equals("F"))
                .collect(Collectors.toList()).get(0);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        SplitPartition splitPartition = splitPartition(stateStore, table);
        splitPartition.splitPartition(partitionForKey1, partitionToFiles.get(partitionForKey1.getId()));

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("key1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                true, false).add("F").build());
        valueSets.put("key2", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, true, 8, true)));

        Constraints queryConstraints = createConstraints(valueSets);
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                new GetTableLayoutRequest(TestUtils.createIdentity(),
                        "abc", "cde",
                        tableName,
                        queryConstraints, getTableResponse.getSchema(),
                        getTableResponse.getPartitionColumns()));

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(2);
    }

    @Test
    public void shouldReturnLeftPartitionWhenItHasBeenSplitBySystemAndRangeMaxIsEqualToThePartitionMax() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);
        IteratorApplyingMetadataHandler sleeperMetadataHandler = handler(instance);
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        StateStore stateStore = stateStore(instance, table);
        Partition partitionForKey1 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("key1").getMin().equals("F"))
                .collect(Collectors.toList()).get(0);

        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        SplitPartition splitPartition = splitPartition(stateStore, table);
        splitPartition.splitPartition(partitionForKey1, partitionToFiles.get(partitionForKey1.getId()));

        Partition firstHalf = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("key1").getMin().equals("F"))
                .filter(p -> p.getRegion().getRange("key2").getMax() != null)
                .collect(Collectors.toList()).get(0);

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("key1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                true, false).add("F").build());
        valueSets.put("key2", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, true, firstHalf.getRegion().getRange("key2").getMax(), false)));

        Constraints queryConstraints = createConstraints(valueSets);
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(), new GetTableLayoutRequest(
                TestUtils.createIdentity(),
                "abc", "cde",
                tableName,
                queryConstraints, getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns()));

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isOne();
        FieldReader key1Reader = partitions.getFieldReader("_MaxRowKey-key1");
        FieldReader key2Reader = partitions.getFieldReader("_MaxRowKey-key2");
        FieldReader key3Reader = partitions.getFieldReader("_MaxRowKey-key3");
        assertThat(key1Reader.readObject().toString()).isEqualTo("G");
        assertThat(key2Reader.readObject()).isEqualTo(firstHalf.getRegion().getRange("key2").getMax());
        assertThat(key3Reader.readObject()).isNull();
    }

    @Test
    public void shouldAddRightPartitionWhenItHasBeenSplitBySystemAndRequestedValueEqualsToThePartitionMax() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);
        IteratorApplyingMetadataHandler sleeperMetadataHandler = handler(instance);
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        StateStore stateStore = stateStore(instance, table);
        Partition partitionForKey1 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("key1").getMin().equals("F"))
                .collect(Collectors.toList()).get(0);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        SplitPartition splitPartition = splitPartition(stateStore, table);
        splitPartition.splitPartition(partitionForKey1, partitionToFiles.get(partitionForKey1.getId()));

        Partition firstHalf = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> p.getRegion().getRange("key1").getMin().equals("F"))
                .filter(p -> p.getRegion().getRange("key2").getMax() != null)
                .collect(Collectors.toList()).get(0);

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("key1", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                true, false).add("F").build());
        valueSets.put("key2", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(firstHalf.getRegion().getRange("key2").getMax()).build());

        Constraints queryConstraints = createConstraints(valueSets);
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(), new GetTableLayoutRequest(
                TestUtils.createIdentity(),
                "abc", "cde",
                tableName,
                queryConstraints, getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns()));

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isOne();
        FieldReader key1Reader = partitions.getFieldReader("_MinRowKey-key1");
        FieldReader key2Reader = partitions.getFieldReader("_MinRowKey-key2");
        FieldReader key3Reader = partitions.getFieldReader("_MinRowKey-key3");
        assertThat(key1Reader.readObject().toString()).isEqualTo("F");
        assertThat(key2Reader.readObject()).isEqualTo(firstHalf.getRegion().getRange("key2").getMax());
        assertThat(key3Reader.readObject()).isEqualTo(Integer.MIN_VALUE);
    }

    // Arrow returns a string row key as Text, so convert it back to a String.
    private static Object readRowKeyValue(FieldReader reader, Field field) {
        Object value = reader.readObject();
        if (value != null && field.getType() instanceof StringType) {
            return value.toString();
        }
        return value;
    }

    private IteratorApplyingMetadataHandler handler(InstanceProperties instanceProperties) {
        return new IteratorApplyingMetadataHandler(s3Client, dynamoClient,
                instanceProperties.get(CONFIG_BUCKET),
                mock(EncryptionKeyFactory.class), mock(SecretsManagerClient.class), mock(AthenaClient.class),
                "spillBucket", "spillPrefix");
    }

    private StateStore stateStore(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private SplitPartition splitPartition(StateStore stateStore, TableProperties tableProperties) {
        return new SplitPartition(stateStore, tableProperties,
                new LocalFileSystemSketchesStore(),
                () -> UUID.randomUUID().toString(), null);
    }

    private void validateSplit(Set<Split> splits, Integer expectedValue) {
        long matched = splits.stream()
                .filter(split -> split.getProperty("_MinRowKey-key1").equals(expectedValue.toString()))
                .peek(split -> {
                    assertThat(split.getProperty("_MaxRowKey-key1")).isEqualTo(Integer.toString(expectedValue + 1));
                    assertThat(split.getProperty("_MinRowKey-key2")).isEmpty();
                    assertThat(split.getProperty("_MaxRowKey-key2")).isNull();
                    assertThat(split.getProperty(RELEVANT_FILES_FIELD)).isEqualTo("[\"s3a://table/partition-" + expectedValue + "/file1.parquet\"," +
                            "\"s3a://table/partition-" + expectedValue + "/file2.parquet\"]");
                })
                .count();

        if (matched != 1) {
            fail("No split in splits matched expected value: " + expectedValue + ". Splits: " + splits);
        }
    }

    private void addPartition(Block partitions, int position, int value) {
        partitions.setRowCount(partitions.getRowCount() + 1);
        BlockUtils.setValue(partitions.getFieldVector("_MinRowKey-key1"), position, value);
        BlockUtils.setValue(partitions.getFieldVector("_MaxRowKey-key1"), position, value + 1);
        BlockUtils.setValue(partitions.getFieldVector("_MinRowKey-key2"), position, "");
        BlockUtils.setValue(partitions.getFieldVector("_MaxRowKey-key2"), position, null);
        BlockUtils.setValue(partitions.getFieldVector(RELEVANT_FILES_FIELD), position, new Gson().toJson(Lists.newArrayList(
                "s3a://table/partition-" + value + "/file1.parquet",
                "s3a://table/partition-" + value + "/file2.parquet")));
    }
}
