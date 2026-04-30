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
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.athena.TestUtils;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.statestore.StateStoreFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.athena.TestUtils.createConstraints;
import static sleeper.athena.metadata.SleeperMetadataHandler.RELEVANT_FILES_FIELD;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class SimpleRecordHandlerIT extends RecordHandlerITBase {

    @Test
    public void shouldReturnNoRecordsWhenFileDoesNotContainExactValue() throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        String file = stateStore.getFileReferences().get(0).getFilename();

        SimpleRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("month", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(2).build());
        predicates.put("day", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(30).build());

        RecordResponse response = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, file)
                        .build(),
                createConstraints(predicates),
                1_000_000L,
                1_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isZero();
    }

    @Test
    public void shouldReturnNoRecordsWhenFileDoesNotContainRange() throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        String file = stateStore.getFileReferences().get(0).getFilename();

        SimpleRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("year", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                2022, true, 2024, false)));

        RecordResponse response = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, file)
                        .build(),
                createConstraints(predicates),
                1_000_000L,
                1_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isZero();
    }

    @Test
    public void shouldReturnSomeRecordsWhenFileContainsPartOfRange() throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        String file2018 = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .findAny()
                .orElseThrow(RuntimeException::new);

        SimpleRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("year", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                2018, true, 2020, false)));
        predicates.put("month", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                6, true, 8, false)));

        RecordResponse response = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, file2018)
                        .build(),
                createConstraints(predicates),
                1_000_000L,
                1_000_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isEqualTo(61);
    }

    @Test
    public void shouldFilterOnValueFields() throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        String file = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .findAny()
                .orElseThrow(RuntimeException::new);

        SimpleRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("str", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                "2018-01-05", true, "2018-01-10", true)));

        RecordResponse response = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, file)
                        .build(),
                createConstraints(predicates),
                1_000_000L,
                1_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isEqualTo(6);
        Block records = ((ReadRecordsResponse) response).getRecords();
        assertFieldContainedValue(records, 0, "str", new Text("2018-01-05"));
        assertFieldContainedValue(records, 5, "str", new Text("2018-01-10"));
    }

    @Test
    public void shouldReturnAllValuesFromFileWhenNoConstraintsArePresent() throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        String file = stateStore.getFileReferences().get(0).getFilename();

        SimpleRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        RecordResponse response = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, file)
                        .build(),
                createConstraints(new HashMap<>()),
                1_000_000L,
                1_000_000L));

        // Then
        ParquetReaderIterator parquetReaderIterator = new ParquetReaderIterator(
                ParquetRowReaderFactory.parquetRowReaderBuilder(new Path(file), SCHEMA).build());
        while (parquetReaderIterator.hasNext()) {
            parquetReaderIterator.next();
        }

        long numberOfRecords = parquetReaderIterator.getNumberOfRowsRead();

        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isEqualTo(numberOfRecords);
    }

    @Test
    public void shouldNotBringBackValueIfItWasNotAskedFor() throws Exception {
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        String file = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                // Ensure the partition has a single file, otherwise the file might not contain the entirety of Feb
                .filter(list -> list.size() == 1)
                .flatMap(List::stream)
                .findAny()
                .orElseThrow(RuntimeException::new);

        SimpleRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("month", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(2).build());

        org.apache.arrow.vector.types.pojo.Schema schemaWithoutDay = new org.apache.arrow.vector.types.pojo.Schema(
                createArrowSchema().getFields()
                        .stream()
                        .filter(field -> !field.getName().equals("day"))
                        .collect(Collectors.toList()));

        RecordResponse response = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                schemaWithoutDay,
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, file)
                        .build(),
                createConstraints(predicates),
                1_000_000L,
                1_000_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isEqualTo(28);
        Block records = ((ReadRecordsResponse) response).getRecords();
        // Just to show the difference
        assertThat(records.getFieldVector("month")).isNotNull();
        assertThat(records.getFieldVector("day")).isNull();
    }

    @Test
    public void shouldReturnNullForNullableStringValueField() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType(), true))
                .build();
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createEmptyTable(instanceProperties, schema);

        Row rowWithValue = new Row();
        rowWithValue.put("key", "present");
        rowWithValue.put("value", "hello");
        Row rowWithNull = new Row();
        rowWithNull.put("key", "absent");
        rowWithNull.put("value", null);
        ingestRows(instanceProperties, tableProperties, List.of(rowWithValue, rowWithNull));

        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        String file = stateStore.getFileReferences().get(0).getFilename();
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new SchemaBuilder()
                .addStringField("key")
                .addStringField("value")
                .build();

        // When
        RecordResponse response = handler(instanceProperties).doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(), "abc", UUID.randomUUID().toString(),
                new TableName(tableProperties.get(TABLE_NAME), tableProperties.get(TABLE_NAME)),
                arrowSchema,
                Split.newBuilder(S3SpillLocation.newBuilder().withBucket(SPILL_BUCKET_NAME).build(), null)
                        .add(RELEVANT_FILES_FIELD, file).build(),
                createConstraints(new HashMap<>()),
                1_000_000L, 1_000_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        Block records = ((ReadRecordsResponse) response).getRecords();
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isEqualTo(2);

        FieldReader keyReader = records.getFieldReader("key");
        FieldReader valueReader = records.getFieldReader("value");
        for (int i = 0; i < 2; i++) {
            keyReader.setPosition(i);
            valueReader.setPosition(i);
            if ("absent".equals(keyReader.readObject().toString())) {
                assertThat(valueReader.readObject()).isNull();
            } else {
                assertThat(valueReader.readObject()).isEqualTo(new Text("hello"));
            }
        }
    }

    private SimpleRecordHandler handler(InstanceProperties instanceProperties) {
        return new SimpleRecordHandler(
                s3Client, dynamoClient,
                instanceProperties.get(CONFIG_BUCKET),
                mock(SecretsManagerClient.class), mock(AthenaClient.class));
    }

    private void ingestRows(InstanceProperties instanceProperties, TableProperties tableProperties, List<Row> rows) throws Exception {
        IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(createTempDirectory(tempDir, null).toString())
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                .hadoopConfiguration(new Configuration())
                .instanceProperties(instanceProperties)
                .build()
                .ingestFromRowIterator(tableProperties, rows.iterator());
    }
}
