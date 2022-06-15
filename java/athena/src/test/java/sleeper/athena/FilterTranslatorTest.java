/*
 * Copyright 2022 Crown Copyright
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
package sleeper.athena;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.Types;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FilterTranslatorTest {

    @ClassRule
    public static TemporaryFolder tempDir = new TemporaryFolder();

    private static final Schema SCHEMA = new Schema();

    static {
        SCHEMA.setRowKeyFields(new Field("int", new IntType()));
        SCHEMA.setSortKeyFields(new Field("long", new LongType()));
        SCHEMA.setValueFields(new Field("string", new StringType()),
                new Field("bytes", new ByteArrayType()),
                new Field("list", new ListType(new StringType())));
    }

    @Test
    public void shouldTranslateNullValueSetIntoNull() {
        // When
        FilterPredicate filterPredicate = new FilterTranslator(new Schema()).toPredicate(null);

        // Then
        assertNull(filterPredicate);
    }

    @Test
    public void shouldTranslateEmptyMapIntoNull() {
        // When
        FilterPredicate filterPredicate = new FilterTranslator(new Schema()).toPredicate(new HashMap<>());

        // Then
        assertNull(filterPredicate);
    }

    @Test
    public void shouldCreateIntegerRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        1, true, 3, false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 3);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldCreateLongRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("long", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.BIGINT.getType(),
                        1_000_000_000L, true, 3_000_000_000L, false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 3);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldCreateStringRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("string", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                        "1", true, "2", false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldFilterUsingMoreThanOneColumn() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("string", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.VARCHAR.getType(),
                        "1", true, "2", false)));
        summary.put("int", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        1, true, 3, false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldFilterUsingMoreThanOneRangeOnTheSameColumn() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        1, true, 3, false),
                Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                                5, true, 8, false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 2, 5, 6, 7);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldCreateByteArrayRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.VARBINARY.getType(),
                        "1".getBytes(), true, "2".getBytes(), false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);

    }

    @Test
    public void shouldCreatePredicateWhenOnlyLowerBoundRangeIsGiven() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(Range
                        .greaterThanOrEqual(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                                15)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(15, 16, 17, 18, 19);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldConsiderBoundsWhenCreatingFilter() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        1, false, 3, true)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(2, 4);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldReturnNullWhenAndingTwoNulls() {
        // When
        FilterPredicate and = FilterTranslator.and(null, null);

        // Then
        assertNull(and);
    }

    @Test
    public void shouldReturnFirstArgumentWhenSecondArgumentIsNullForAnd() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate and = FilterTranslator.and(lhs, null);

        // Then
        assertEquals(lhs, and);
    }

    @Test
    public void shouldReturnSecondArgumentWhenFirstArgumentIsNullForAnd() {
        // Given
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate and = FilterTranslator.and(null, rhs);

        // Then
        assertEquals(rhs, and);
    }

    @Test
    public void shouldAndTwoArgumentsWhenBothNotNullForAnd() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("other"), 1);
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate and = FilterTranslator.and(lhs, rhs);

        // Then
        assertEquals(FilterApi.and(lhs, rhs), and);
    }

    @Test
    public void shouldReturnNullWhenOringTwoNulls() {
        // When
        FilterPredicate or = FilterTranslator.or(null, null);

        // Then
        assertNull(or);
    }

    @Test
    public void shouldReturnFirstArgumentWhenSecondArgumentIsNullForOr() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate or = FilterTranslator.or(lhs, null);

        // Then
        assertEquals(lhs, or);
    }

    @Test
    public void shouldReturnSecondArgumentWhenFirstArgumentIsNullForOr() {
        // Given
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate or = FilterTranslator.or(null, rhs);

        // Then
        assertEquals(rhs, or);
    }

    @Test
    public void shouldOrTwoArgumentsWhenBothNotNullForOr() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("other"), 1);
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate or = FilterTranslator.or(lhs, rhs);

        // Then
        assertEquals(FilterApi.or(lhs, rhs), or);
    }

    @Test
    public void shouldHandleExactValueAllowListForIntegers() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), true, false)
                .add(1).add(3).add(5).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1,3,5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueDenyListForIntegers() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), false, false)
                .add(0).add(1).add(2).add(3).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(4, 20);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueAllowListForLongs() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("long", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.BIGINT.getType(), true, false)
                .add(1_000_000_000L).add(3_000_000_000L).add(5_000_000_000L).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1,3,5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueDenyListForLongs() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("long", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.BIGINT.getType(), false, false)
                .add(0L).add(1_000_000_000L).add(2_000_000_000L).add(3_000_000_000L).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(4, 20);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueAllowListForStrings() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("string", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false)
                .add("1").add("3").add("5").build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1,3,5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueDenyListForStrings() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("string", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), false, false)
                .add("0").add("1").add("2").add("3").build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(4, 20);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueAllowListForByteArrays() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARBINARY.getType(), true, false)
                .add("1".getBytes()).add("3".getBytes()).add("5".getBytes()).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1,3,5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleExactValueDenyListForByteArrays() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARBINARY.getType(), false, false)
                .add("0".getBytes()).add("1".getBytes()).add("2".getBytes()).add("3".getBytes()).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(4, 20);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldHandleDifferentPredicateTypesOverMultipleFields() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(
                Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        4, false, 8, false)));

        summary.put("string", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), false, false)
                .add("5").add("7").build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(6);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expectedRecords, actualRecords);
    }

    @Test
    public void shouldReturnNothingIfPredicatesDontOverlap() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(
                Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        4, false, 8, false)));

        summary.put("string", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false)
                .add("9").add("10").build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(new ArrayList<>(), actualRecords);
    }

    @Test
    public void shouldReturnNullIfPredicateIsAll() throws IOException {
        // Given
        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", new AllOrNoneValueSet(Types.MinorType.VARBINARY.getType(), true, true));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        assertNull(filterPredicate);
    }

    @Test
    public void shouldDealWithExactBoundedRanges() throws IOException {
        // Given
        String dataFile = new File(tempDir.newFolder(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", SortedRangeSet.of(
                Range.range(new BlockAllocatorImpl(), Types.MinorType.INT.getType(),
                        4, true, 4, true)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expected = generateRecords(4);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertEquals(expected, actualRecords);
    }

    private List<Record> readData(String dataFile, FilterPredicate filterPredicate) throws IOException {
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(dataFile), SCHEMA)
                .withFilter(FilterCompat.get(filterPredicate))
                .build();

        List<Record> records = new ArrayList<>();
        ParquetReaderIterator parquetReaderIterator = new ParquetReaderIterator(reader);

        while (parquetReaderIterator.hasNext()) {
            records.add(parquetReaderIterator.next());
        }

        reader.close();
        return records;
    }

    private void writeData(String dataDir) throws IOException {
        ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(new Path(dataDir), SchemaConverter.getSchema(SCHEMA), SCHEMA)
                .build();

        generateRecords(0, 20).forEach(record -> {
            try {
                writer.write(record);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writer.close();
    }

    private List<Record> generateRecords(int min, int max) {
        List<Record> records = new ArrayList<>();

        for (int i = min; i < max; i++) {
            Record record = createRecord(i);
            records.add(record);
        }

        return records;
    }

    private List<Record> generateRecords(Integer... values) {
        List<Record> records = new ArrayList<>();

        for (int i : values) {
            Record record = createRecord(i);
            records.add(record);
        }

        return records;
    }

    private Record createRecord(int i) {
        Record record = new Record();
        record.put("int", i);
        record.put("long", i * 1_000_000_000L);
        record.put("string", Integer.toString(i));
        record.put("bytes", Integer.toString(i).getBytes());
        record.put("list", Lists.newArrayList(Integer.toString(i)));
        return record;
    }
}
