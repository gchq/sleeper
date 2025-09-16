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
package sleeper.athena.record;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
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
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import sleeper.athena.TestUtils;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.ConfigStringIterator;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class IteratorApplyingRecordHandlerIT extends RecordHandlerITBase {

    @Test
    public void shouldReturnNoResultsIfPartitionDoesNotContainExactValue() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<String> partition2018Files = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        IteratorApplyingRecordHandler sleeperRecordHandler = handler(instanceProperties);

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
                        .add(RELEVANT_FILES_FIELD, new Gson().toJson(partition2018Files))
                        .add(MIN_ROW_KEY_PREFIX + 0, "2018")
                        .add(MAX_ROW_KEY_PREFIX + 0, "2019")
                        .add(MIN_ROW_KEY_PREFIX + 1, MIN_VALUE)
                        .add(MIN_ROW_KEY_PREFIX + 2, MIN_VALUE)
                        .build(),
                createConstraints(predicates),
                1_000_000L,
                1_000L));

        // Then
        assertThat(response).isInstanceOf(ReadRecordsResponse.class);
        assertThat(((ReadRecordsResponse) response).getRecordCount()).isZero();
    }

    @Test
    public void shouldReturnRecordCorrectlyIfPartitionDoesContainExactValue() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<String> partition2018Files = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        IteratorApplyingRecordHandler sleeperRecordHandler = handler(instanceProperties);

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
                .add(28).build());

        RecordResponse rawResponse = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, new Gson().toJson(partition2018Files))
                        .add(MIN_ROW_KEY_PREFIX + 0, "2018")
                        .add(MAX_ROW_KEY_PREFIX + 0, "2019")
                        .add(MIN_ROW_KEY_PREFIX + 1, MIN_VALUE)
                        .add(MIN_ROW_KEY_PREFIX + 2, MIN_VALUE)
                        .build(),
                createConstraints(predicates),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE));

        // Then
        assertThat(rawResponse).isInstanceOf(ReadRecordsResponse.class);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isOne();
        Block records = response.getRecords();
        assertRecordContainedDay(records, 0, 2018, Month.FEBRUARY, 28);
    }

    @Test
    public void shouldReturnRecordCorrectlyIfPartitionDoesContainedAllOfARange() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2018, 2019, 2020);

        // When
        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<String> partition2018Files = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        IteratorApplyingRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("year", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                2018, true, 2019, false)));
        predicates.put("month", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, false, 6, true)));

        RecordResponse rawResponse = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, new Gson().toJson(partition2018Files))
                        .add(MIN_ROW_KEY_PREFIX + 0, "2018")
                        .add(MAX_ROW_KEY_PREFIX + 0, "2019")
                        .add(MIN_ROW_KEY_PREFIX + 1, MIN_VALUE)
                        .add(MIN_ROW_KEY_PREFIX + 2, MIN_VALUE)
                        .build(),
                createConstraints(predicates),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE));

        // Then
        assertThat(rawResponse).isInstanceOf(ReadRecordsResponse.class);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isEqualTo(30);
        Block records = response.getRecords();
        assertRecordContainedDay(records, 0, 2018, Month.JUNE, 1);
        assertRecordContainedDay(records, 29, 2018, Month.JUNE, 30);
    }

    @Test
    public void shouldHandlePartitionsWhichContainNoFiles() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createEmptyTable(instanceProperties, 2016, 2017, 2018);

        // When
        List<String> partition2016Files = new ArrayList<>();

        IteratorApplyingRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();

        RecordResponse rawResponse = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null).add(RELEVANT_FILES_FIELD, new Gson().toJson(partition2016Files))
                        .add(MIN_ROW_KEY_PREFIX + 0, "2016")
                        .add(MAX_ROW_KEY_PREFIX + 0, "2017")
                        .add(MIN_ROW_KEY_PREFIX + 1, MIN_VALUE)
                        .add(MIN_ROW_KEY_PREFIX + 2, MIN_VALUE)
                        .build(),
                createConstraints(predicates),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE));

        // Then
        assertThat(rawResponse).isInstanceOf(ReadRecordsResponse.class);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isZero();
    }

    @Test
    public void shouldHandleStringRowKeyTypes() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();

        TableProperties tableProperties = createEmptyTable(instanceProperties, schema);

        // When
        List<String> emptyFiles = new ArrayList<>();

        IteratorApplyingRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();

        RecordResponse rawResponse = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null).add(RELEVANT_FILES_FIELD, new Gson().toJson(emptyFiles))
                        .add(MIN_ROW_KEY_PREFIX + 0, "")
                        .add(MAX_ROW_KEY_PREFIX + 0, null)
                        .build(),
                createConstraints(predicates),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE));

        // Then
        assertThat(rawResponse).isInstanceOf(ReadRecordsResponse.class);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isZero();
    }

    @Test
    public void shouldApplyCompactionIteratorToResultsIfConfigured() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(instanceProperties, 2016, 2017, 2018);

        // When
        tableProperties.set(ITERATOR_CLASS_NAME, CountAggregator.class.getName());
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);

        StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
        Map<String, List<String>> partitionToFiles = stateStore.getPartitionToReferencedFilesMap();
        List<String> partition2018Files = stateStore.getLeafPartitions().stream()
                .filter(p -> (Integer) p.getRegion().getRange("year").getMin() == 2018)
                .map(Partition::getId)
                .map(partitionToFiles::get)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        IteratorApplyingRecordHandler sleeperRecordHandler = handler(instanceProperties);

        String tableName = tableProperties.get(TABLE_NAME);
        S3SpillLocation spillLocation = S3SpillLocation.newBuilder()
                .withBucket(SPILL_BUCKET_NAME)
                .build();

        Map<String, ValueSet> predicates = new HashMap<>();
        predicates.put("month", EquatableValueSet
                .newBuilder(new BlockAllocatorImpl(), Types.MinorType.INT.getType(), true, false)
                .add(3).build());
        predicates.put("day", SortedRangeSet.of(Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                5, false, 8, true)));

        RecordResponse rawResponse = sleeperRecordHandler.doReadRecords(new BlockAllocatorImpl(), new ReadRecordsRequest(
                TestUtils.createIdentity(),
                "abc",
                UUID.randomUUID().toString(),
                new TableName(tableName, tableName),
                createArrowSchema(),
                Split.newBuilder(spillLocation, null)
                        .add(RELEVANT_FILES_FIELD, new Gson().toJson(partition2018Files))
                        .add(MIN_ROW_KEY_PREFIX + 0, "2018")
                        .add(MAX_ROW_KEY_PREFIX + 0, "2019")
                        .add(MIN_ROW_KEY_PREFIX + 1, MIN_VALUE)
                        .add(MIN_ROW_KEY_PREFIX + 2, MIN_VALUE)
                        .build(),
                createConstraints(predicates),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE));

        // Then
        assertThat(rawResponse).isInstanceOf(ReadRecordsResponse.class);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertThat(response.getRecordCount()).isEqualTo(3);

        Block records = response.getRecords();

        // First one should just be the normal count
        long firstCount = 2018 * 3 * 6;
        assertFieldContainedValue(records, 0, "count", firstCount);

        // Second should be the first plus second
        long secondCount = firstCount + 2018 * 3 * 7;
        assertFieldContainedValue(records, 1, "count", secondCount);

        // Third should be aggregated second plus third
        long thirdCount = secondCount + 2018 * 3 * 8;
        assertFieldContainedValue(records, 2, "count", thirdCount);
    }

    private IteratorApplyingRecordHandler handler(InstanceProperties instanceProperties) {
        return new IteratorApplyingRecordHandler(
                s3Client, dynamoClient,
                instanceProperties.get(CONFIG_BUCKET),
                mock(SecretsManagerClient.class), mock(AthenaClient.class));
    }

    private void assertRecordContainedDay(Block records, int position, int year, Month month, int day) {
        assertFieldContainedValue(records, position, "year", year);
        assertFieldContainedValue(records, position, "month", month.getValue());
        assertFieldContainedValue(records, position, "day", day);
        LocalDate date = LocalDate.of(year, month, day);
        long timestamp = Date.from(Timestamp.valueOf(date.atStartOfDay()).toInstant()).getTime();
        assertFieldContainedValue(records, position, "timestamp", timestamp);
        assertFieldContainedValue(records, position, "count", (long) year * (long) month.getValue() * (long) day);
        assertFieldContainedValue(records, position, "str", new Text(date.toString()));
        assertFieldContainedValue(records, position, "list", Lists.newArrayList(new Text(date.getEra().toString())));
        /*
         * When Maps are supported, test it here
         */
    }

    /**
     * Simple iterator which adds the count of the previous row to the current one.
     */
    public static class CountAggregator implements ConfigStringIterator {

        @Override
        public void init(String configString, Schema schema) {
            // no op
        }

        @Override
        public List<String> getRequiredValueFields() {
            return Collections.singletonList("count");
        }

        @Override
        public CloseableIterator<Row> applyTransform(CloseableIterator<Row> rowCloseableIterator) {
            return new CountAggregatorIteratorImpl(rowCloseableIterator);
        }

        private static class CountAggregatorIteratorImpl implements CloseableIterator<Row> {
            private final CloseableIterator<Row> rows;
            private Row previous = null;

            private CountAggregatorIteratorImpl(CloseableIterator<Row> consumedRows) {
                this.rows = consumedRows;
            }

            @Override
            public void close() throws IOException {
                rows.close();
            }

            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public Row next() {
                Row current = rows.next();
                if (previous != null) {
                    current.put("count", (long) current.get("count") + (long) previous.get("count"));
                }
                previous = current;
                return current;
            }
        }
    }

}
