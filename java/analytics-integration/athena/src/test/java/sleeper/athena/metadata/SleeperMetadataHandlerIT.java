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
package sleeper.athena.metadata;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
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
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.athena.TestUtils;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.splitter.core.split.SplitPartition;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.athena.TestUtils.createConstraints;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class SleeperMetadataHandlerIT extends MetadataHandlerITBase {

    @Test
    public void shouldJustReturnLeafPartitionsWhichContainValuesGreaterThanMinKey() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        StateStore stateStore = stateStore(instance, table);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<String> relevantFiles = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() >= 2020)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));

        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(),
                        "abc", "def", tableName, new HashMap<>()));

        BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl();
        Map<String, ValueSet> predicate = new HashMap<>();
        predicate.put("year", SortedRangeSet.of(Range.greaterThanOrEqual(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), 2020)));

        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(predicate),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(blockAllocator, request);

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isOne();
        FieldReader partitionReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        partitionReader.setPosition(0);
        List<String> files = (List<String>) new Gson().fromJson(partitionReader.readObject().toString(), List.class);
        assertThat(files).isEqualTo(relevantFiles);
    }

    @Test
    public void shouldJustReturnPartitionsWhichContainValuesLessThanMaxKey() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);
        StateStore stateStore = stateStore(instance, table);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<List<String>> relevantFiles = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() <= 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));

        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        BlockAllocatorImpl blockAllocator = new BlockAllocatorImpl();
        Map<String, ValueSet> predicate = new HashMap<>();
        predicate.put("year", SortedRangeSet.of(Range.lessThanOrEqual(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), 2018)));

        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(predicate),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(blockAllocator, request);

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(2);
        FieldReader partitionReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        partitionReader.setPosition(0);
        Object files = new Gson().fromJson(partitionReader.readObject().toString(), List.class);
        assertThat(files).isEqualTo(relevantFiles.get(0));
        partitionReader.setPosition(1);
        files = new Gson().fromJson(partitionReader.readObject().toString(), List.class);
        assertThat(files).isEqualTo(relevantFiles.get(1));
    }

    @Test
    public void shouldJustReturnPartitionsThatContainASpecificKey() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        StateStore stateStore = stateStore(instance, table);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<List<String>> relevantFiles = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));

        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        Map<String, ValueSet> predicate = new HashMap<>();
        predicate.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(2018).build());
        predicate.put("month", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(12).build());
        predicate.put("day", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(25).build());

        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(predicate),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                request);

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isOne();
        FieldReader partitionReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        partitionReader.setPosition(0);
        Object files = new Gson().fromJson(partitionReader.readObject().toString(), List.class);
        assertThat(files).isEqualTo(relevantFiles.get(0));
    }

    @Test
    public void shouldNotFilterPartitionsBasedOnDenyList() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        StateStore stateStore = stateStore(instance, table);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<List<String>> relevantFiles = stateStore.getLeafPartitions().stream()
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));

        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        Map<String, ValueSet> predicate = new HashMap<>();
        predicate.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                false, false).add(2018).build());

        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(predicate),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                request);

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(4);
        FieldReader partitionFilesReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        for (int i = 0; i < 4; i++) {
            partitionFilesReader.setPosition(i);
            Object o = new Gson().fromJson(partitionFilesReader.readObject().toString(), List.class);
            assertThat(o).isEqualTo(relevantFiles.get(i));
        }
    }

    @Test
    public void shouldScanAllFilesWhenANonKeyFieldIsFiltered() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        StateStore stateStore = stateStore(instance, table);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<List<String>> relevantFiles = stateStore.getLeafPartitions().stream()
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));
        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        Map<String, ValueSet> predicate = new HashMap<>();
        predicate.put("count", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(1).add(2).add(3).build());

        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(predicate),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                request);

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(4);

        FieldReader partitionFilesReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        for (int i = 0; i < 4; i++) {
            partitionFilesReader.setPosition(i);
            Object o = new Gson().fromJson(partitionFilesReader.readObject().toString(), List.class);
            assertThat(o).isEqualTo(relevantFiles.get(i));
        }
    }

    @Test
    public void shouldGenerateArrowSchemaFromSleeperSchema() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        String tableName = createEmptyTable(instance).get(TABLE_NAME);

        // When
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", new TableName("unused", tableName), new HashMap<>()));

        // Then
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new SchemaBuilder()
                .addIntField("year")
                .addIntField("month")
                .addIntField("day")
                .addBigIntField("count")
                .build();

        org.apache.arrow.vector.types.pojo.Schema schema = getTableResponse.getSchema();
        assertThat(schema).isEqualTo(arrowSchema);
    }

    @Test
    public void shouldReturnMultiplePartitionsWhenExactQueryMatchesMultiplePartitions() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);

        // When
        // Make query
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        StateStore stateStore = stateStore(instance, table);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<List<String>> relevantFiles = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == Integer.MIN_VALUE || (Integer) p.getRegion().getRange("year").getMin() == 2019)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        TableName tableName = new TableName(table.get(TABLE_NAME), table.get(TABLE_NAME));

        GetTableResponse getTableResponse = sleeperMetadataHandler.doGetTable(new BlockAllocatorImpl(),
                new GetTableRequest(TestUtils.createIdentity(), "abc", "def", tableName, new HashMap<>()));

        Map<String, ValueSet> predicate = new HashMap<>();
        predicate.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(2017).add(2019).build());

        GetTableLayoutRequest request = new GetTableLayoutRequest(TestUtils.createIdentity(),
                "abc",
                "def",
                tableName,
                createConstraints(predicate),
                getTableResponse.getSchema(),
                getTableResponse.getPartitionColumns());

        GetTableLayoutResponse getTableLayoutResponse = sleeperMetadataHandler.doGetTableLayout(new BlockAllocatorImpl(),
                request);

        // Then
        Block partitions = getTableLayoutResponse.getPartitions();
        assertThat(partitions.getRowCount()).isEqualTo(2);

        FieldReader partitionReader = partitions.getFieldReader(RELEVANT_FILES_FIELD);
        for (int i = 0; i < 2; i++) {
            partitionReader.setPosition(i);
            assertThat(new Gson().fromJson(partitionReader.readObject().toString(), List.class)).isEqualTo(relevantFiles.get(i));
        }

        assertThat(sleeperMetadataHandler.writeExtraPartitionDataCalled).isEqualTo(2);
    }

    @Test
    public void shouldProvideSetContainingInstanceIdWhenAskedForSchemaList() throws Exception {
        // Given
        InstanceProperties instance = createInstance();

        // When
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        ListSchemasResponse listSchemasResponse = sleeperMetadataHandler.doListSchemaNames(new BlockAllocatorImpl(), new ListSchemasRequest(TestUtils.createIdentity(), "abc", "def"));

        // Then
        assertThat(listSchemasResponse.getSchemas()).containsExactly(instance.get(ID));
    }

    @Test
    public void shouldJustReturnAllTheTablesWithinTheInstanceWhenAskedToListTheTables() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        String table1 = createEmptyTable(instance).get(TABLE_NAME);
        String table2 = createEmptyTable(instance).get(TABLE_NAME);
        String table3 = createEmptyTable(instance).get(TABLE_NAME);

        // When
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        ListTablesResponse listTablesResponse = sleeperMetadataHandler.doListTables(new BlockAllocatorImpl(),
                new ListTablesRequest(TestUtils.createIdentity(), "abc", "def", "mySchema", "next", -1));

        // Then
        assertThat(listTablesResponse.getTables()).containsExactlyInAnyOrder(
                new TableName("mySchema", table1),
                new TableName("mySchema", table2),
                new TableName("mySchema", table3));
    }

    @Test
    public void shouldProvideSubsetOfTheTablesWithinTheInstanceWhenAskedToListTheTablesAndPageSizeIsSet() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        String table1 = createEmptyTable(instance).get(TABLE_NAME);
        String table2 = createEmptyTable(instance).get(TABLE_NAME);

        // When
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        ListTablesResponse listTablesResponse = sleeperMetadataHandler.doListTables(new BlockAllocatorImpl(),
                new ListTablesRequest(TestUtils.createIdentity(), "abc", "def", "mySchema", null, 1));

        // Then

        // Order the tables
        List<String> sorted = Lists.newArrayList(table1, table2).stream().sorted().collect(Collectors.toList());

        assertThat(listTablesResponse.getNextToken()).isEqualTo("1");
        assertThat(listTablesResponse.getTables()).containsExactly(new TableName("mySchema", sorted.get(0)));
    }

    @Test
    public void shouldProvideSubsetOfTheTablesWithinTheInstanceWhenAskedToListTheTablesAndPageSizeIsSetStartingWithStartToken() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        String table1 = createEmptyTable(instance).get(TABLE_NAME);
        String table2 = createEmptyTable(instance).get(TABLE_NAME);

        List<String> sorted = Lists.newArrayList(table1, table2).stream().sorted().collect(Collectors.toList());

        // When
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);

        ListTablesResponse listTablesResponse = sleeperMetadataHandler.doListTables(new BlockAllocatorImpl(),
                new ListTablesRequest(TestUtils.createIdentity(), "abc", "def", "mySchema", "1", 1));

        // Then
        assertThat(listTablesResponse.getNextToken()).isNull();
        assertThat(listTablesResponse.getTables()).containsExactly(new TableName("mySchema", sorted.get(1)));
    }

    @Test
    public void shouldReturnBothPartitionsWhenItHasBeenSplitBySystemAndLeftMaxAppearsInDenyList() throws Exception {
        // Given
        InstanceProperties instance = createInstance();
        TableProperties table = createTable(instance);
        SleeperMetadataHandlerImpl sleeperMetadataHandler = handler(instance);
        TableName tableName = new TableName(instance.get(ID), table.get(TABLE_NAME));

        // When
        StateStore stateStore = stateStore(instance, table);
        Partition partition2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .collect(Collectors.toList()).get(0);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        SplitPartition splitPartition = splitPartition(stateStore, table);
        splitPartition.splitPartition(partition2018, partitionToFiles.get(partition2018.getId()));
        Partition firstHalfOf2018 = stateStore.getLeafPartitions()
                .stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .filter(p -> (Integer) p.getRegion().getRange("month").getMax() != null)
                .collect(Collectors.toList()).get(0);

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("year", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                true, false).add(2018).build());
        valueSets.put("month", EquatableValueSet.newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                false, false).add(firstHalfOf2018.getRegion().getRange("month").getMax()).build());

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
        assertThat(partitions.getRowCount()).isEqualTo(2);
    }

    @Test
    public void shouldCallExtraSchemaEnhancementMethodWhenEnhanceingSchema() throws IOException {
        // Given
        InstanceProperties instance = createInstance();
        SleeperMetadataHandlerImpl handler = handler(instance);

        // When
        handler.enhancePartitionSchema(new SchemaBuilder(), null);

        // Then
        assertThat(handler.schemaEnhancementsCalled).isOne();

    }

    private SleeperMetadataHandlerImpl handler(InstanceProperties instanceProperties) {
        return new SleeperMetadataHandlerImpl(s3Client, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    }

    private StateStore stateStore(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private SplitPartition splitPartition(StateStore stateStore, TableProperties tableProperties) {
        return new SplitPartition(stateStore, tableProperties,
                new LocalFileSystemSketchesStore(),
                () -> UUID.randomUUID().toString(), null);
    }

    private static class SleeperMetadataHandlerImpl extends SleeperMetadataHandler {
        private int schemaEnhancementsCalled = 0;
        private int writeExtraPartitionDataCalled = 0;

        private SleeperMetadataHandlerImpl(S3Client s3Client, DynamoDbClient dynamoDBClient, String configBucket) {
            super(s3Client, dynamoDBClient, configBucket, mock(EncryptionKeyFactory.class),
                    mock(SecretsManagerClient.class), mock(AthenaClient.class), "abc", "def");
        }

        @Override
        protected void addExtraSchemaEnhancements(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request) {
            schemaEnhancementsCalled++;
        }

        @Override
        protected void writeExtraPartitionDataToBlock(Partition partition, Block block, int rowNum) {
            writeExtraPartitionDataCalled++;
        }

        @Override
        public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {
            // Implementation specific no need to test
            return null;
        }
    }
}
