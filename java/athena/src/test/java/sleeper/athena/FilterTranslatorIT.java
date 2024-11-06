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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.record.ParquetRecordWriterFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class FilterTranslatorIT {

    @TempDir
    public static java.nio.file.Path tempDir;

    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(new Field("int", new IntType()))
            .sortKeyFields(new Field("long", new LongType()))
            .valueFields(new Field("string", new StringType()),
                    new Field("bytes", new ByteArrayType()),
                    new Field("list", new ListType(new StringType())))
            .build();

    @Test
    void shouldTranslateNullValueSetIntoNull() {
        // When
        FilterPredicate filterPredicate = new FilterTranslator(SCHEMA).toPredicate(null);

        // Then
        assertThat(filterPredicate).isNull();
    }

    @Test
    void shouldTranslateEmptyMapIntoNull() {
        // When
        FilterPredicate filterPredicate = new FilterTranslator(SCHEMA).toPredicate(new HashMap<>());

        // Then
        assertThat(filterPredicate).isNull();
    }

    @Test
    void shouldCreateIntegerRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldCreateLongRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldCreateStringRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldFilterUsingMoreThanOneColumn() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldFilterUsingMoreThanOneRangeOnTheSameColumn() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldCreateByteArrayRangePredicateFromSortedSetContainingRange() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", SortedRangeSet.of(Range
                .range(new BlockAllocatorImpl(), Types.MinorType.VARBINARY.getType(),
                        "1".getBytes(UTF_8), true, "2".getBytes(UTF_8), false)));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertThat(actualRecords).isEqualTo(expectedRecords);

    }

    @Test
    void shouldCreatePredicateWhenOnlyLowerBoundRangeIsGiven() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldConsiderBoundsWhenCreatingFilter() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldReturnNullWhenAndingTwoNulls() {
        // When
        FilterPredicate and = FilterTranslator.and(null, null);

        // Then
        assertThat(and).isNull();
    }

    @Test
    void shouldReturnFirstArgumentWhenSecondArgumentIsNullForAnd() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate and = FilterTranslator.and(lhs, null);

        // Then
        assertThat(and).isEqualTo(lhs);
    }

    @Test
    void shouldReturnSecondArgumentWhenFirstArgumentIsNullForAnd() {
        // Given
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate and = FilterTranslator.and(null, rhs);

        // Then
        assertThat(and).isEqualTo(rhs);
    }

    @Test
    void shouldAndTwoArgumentsWhenBothNotNullForAnd() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("other"), 1);
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate and = FilterTranslator.and(lhs, rhs);

        // Then
        assertThat(and).isEqualTo(FilterApi.and(lhs, rhs));
    }

    @Test
    void shouldReturnNullWhenOringTwoNulls() {
        // When
        FilterPredicate or = FilterTranslator.or(null, null);

        // Then
        assertThat(or).isNull();
    }

    @Test
    void shouldReturnFirstArgumentWhenSecondArgumentIsNullForOr() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate or = FilterTranslator.or(lhs, null);

        // Then
        assertThat(or).isEqualTo(lhs);
    }

    @Test
    void shouldReturnSecondArgumentWhenFirstArgumentIsNullForOr() {
        // Given
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate or = FilterTranslator.or(null, rhs);

        // Then
        assertThat(or).isEqualTo(rhs);
    }

    @Test
    void shouldOrTwoArgumentsWhenBothNotNullForOr() {
        // Given
        Operators.Eq<Integer> lhs = FilterApi.eq(FilterApi.intColumn("other"), 1);
        Operators.Eq<Integer> rhs = FilterApi.eq(FilterApi.intColumn("test"), 1);

        // When
        FilterPredicate or = FilterTranslator.or(lhs, rhs);

        // Then
        assertThat(or).isEqualTo(FilterApi.or(lhs, rhs));
    }

    @Test
    void shouldHandleExactValueAllowListForIntegers() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("int", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.INT.getType(), true, false)
                .add(1).add(3).add(5).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 3, 5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueDenyListForIntegers() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueAllowListForLongs() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("long", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.BIGINT.getType(), true, false)
                .add(1_000_000_000L).add(3_000_000_000L).add(5_000_000_000L).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 3, 5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueDenyListForLongs() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueAllowListForStrings() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("string", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARCHAR.getType(), true, false)
                .add("1").add("3").add("5").build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 3, 5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueDenyListForStrings() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueAllowListForByteArrays() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARBINARY.getType(), true, false)
                .add("1".getBytes(UTF_8)).add("3".getBytes(UTF_8)).add("5".getBytes(UTF_8)).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(1, 3, 5);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleExactValueDenyListForByteArrays() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
        writeData(dataFile);

        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", EquatableValueSet.newBuilder(new BlockAllocatorImpl(),
                Types.MinorType.VARBINARY.getType(), false, false)
                .add("0".getBytes(UTF_8)).add("1".getBytes(UTF_8)).add("2".getBytes(UTF_8)).add("3".getBytes(UTF_8)).build());

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        List<Record> expectedRecords = generateRecords(4, 20);
        List<Record> actualRecords = readData(dataFile, filterPredicate);

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldHandleDifferentPredicateTypesOverMultipleFields() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expectedRecords);
    }

    @Test
    void shouldReturnNothingIfPredicatesDontOverlap() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(new ArrayList<>());
    }

    @Test
    void shouldReturnNullIfPredicateIsAll() {
        // Given
        FilterTranslator filterTranslator = new FilterTranslator(SCHEMA);

        // When
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("bytes", new AllOrNoneValueSet(Types.MinorType.VARBINARY.getType(), true, true));

        FilterPredicate filterPredicate = filterTranslator.toPredicate(summary);

        // Then
        assertThat(filterPredicate).isNull();
    }

    @Test
    void shouldDealWithExactBoundedRanges() throws IOException {
        // Given
        String dataFile = new File(createTempDirectory(tempDir, null).toString(), "test.parquet").getAbsolutePath();
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

        assertThat(actualRecords).isEqualTo(expected);
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
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(dataDir), SCHEMA);

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
        record.put("bytes", Integer.toString(i).getBytes(UTF_8));
        record.put("list", Lists.newArrayList(Integer.toString(i)));
        return record;
    }
}
