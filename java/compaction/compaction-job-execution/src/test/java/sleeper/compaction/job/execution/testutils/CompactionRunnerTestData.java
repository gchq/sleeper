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
package sleeper.compaction.job.execution.testutils;

import com.facebook.collections.ByteArray;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.LocalFileSystemSketchesStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class CompactionRunnerTestData {

    private CompactionRunnerTestData() {
    }

    public static List<Row> keyAndTwoValuesSortedEvenLongs() {
        return streamKeyAndTwoValuesFromEvens(n -> (long) n)
                .collect(Collectors.toList());
    }

    public static List<Row> keyAndTwoValuesSortedOddLongs() {
        return streamKeyAndTwoValuesFromOdds(n -> (long) n)
                .collect(Collectors.toList());
    }

    public static List<Row> keyAndTwoValuesSortedEvenStrings() {
        return streamKeyAndTwoValuesFromEvens(CompactionRunnerTestData::nthString)
                .collect(Collectors.toList());
    }

    public static List<Row> keyAndTwoValuesSortedOddStrings() {
        return streamKeyAndTwoValuesFromOdds(CompactionRunnerTestData::nthString)
                .collect(Collectors.toList());
    }

    public static List<Row> keyAndTwoValuesSortedEvenByteArrays() {
        return streamKeyAndTwoValuesFromEvens(CompactionRunnerTestData::nthByteArray)
                .collect(Collectors.toList());
    }

    public static List<Row> keyAndTwoValuesSortedOddByteArrays() {
        return streamKeyAndTwoValuesFromOdds(CompactionRunnerTestData::nthByteArray)
                .collect(Collectors.toList());
    }

    private static Stream<Row> streamKeyAndTwoValuesFromEvens(Function<Integer, Object> convert) {
        return streamFromEvens((even, record) -> {
            Object converted = convert.apply(even);
            record.put("key", converted);
            record.put("value1", converted);
            record.put("value2", 987654321L);
        });
    }

    private static Stream<Row> streamKeyAndTwoValuesFromOdds(Function<Integer, Object> convert) {
        Object value1 = convert.apply(1001);
        return streamFromOdds((odd, record) -> {
            record.put("key", convert.apply(odd));
            record.put("value1", value1);
            record.put("value2", 123456789L);
        });
    }

    public static List<Row> specifiedFromEvens(BiConsumer<Integer, Row> setRow) {
        return streamFromEvens(setRow).collect(Collectors.toList());
    }

    public static List<Row> specifiedFromOdds(BiConsumer<Integer, Row> setRow) {
        return streamFromOdds(setRow).collect(Collectors.toList());
    }

    private static Stream<Row> streamFromEvens(BiConsumer<Integer, Row> setRow) {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    int even = 2 * i;
                    Row row = new Row();
                    setRow.accept(even, row);
                    return row;
                });
    }

    private static Stream<Row> streamFromOdds(BiConsumer<Integer, Row> setRow) {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    int odd = 2 * i + 1;
                    Row row = new Row();
                    setRow.accept(odd, row);
                    return row;
                });
    }

    private static String nthString(int n) {
        return ""
                + (char) ('a' + (n / 26))
                + (char) ('a' + (n % 26));
    }

    private static byte[] nthByteArray(int n) {
        return new byte[]{
            (byte) (n / 128),
            (byte) (n % 128)
        };
    }

    public static List<Row> combineSortedBySingleKey(List<Row> data1, List<Row> data2) {
        return combineSortedBySingleKey(data1, data2, record -> record.get("key"));
    }

    public static List<Row> combineSortedBySingleByteArrayKey(List<Row> data1, List<Row> data2) {
        return combineSortedBySingleKey(data1, data2, record -> ByteArray.wrap((byte[]) record.get("key")));
    }

    public static List<Row> combineSortedBySingleKey(List<Row> data1, List<Row> data2, Function<Row, Object> getKey) {
        SortedMap<Object, Row> data = new TreeMap<>();
        data1.forEach(record -> data.put(getKey.apply(record), record));
        data2.forEach(record -> data.put(getKey.apply(record), record));
        return new ArrayList<>(data.values());
    }

    public static FileReference writeRootFile(Schema schema, StateStore stateStore, String filename, List<Row> rows) throws Exception {
        Sketches sketches = Sketches.from(schema);
        try (ParquetWriter<Row> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), schema)) {
            for (Row row : rows) {
                writer.write(row);
                sketches.update(row);
            }
        }
        new LocalFileSystemSketchesStore().saveFileSketches(filename, schema, sketches);
        FileReference fileReference = FileReferenceFactory.from(stateStore).rootFile(filename, rows.size());
        update(stateStore).addFile(fileReference);
        return fileReference;
    }

    public static List<Row> readDataFile(Schema schema, String filename) throws IOException {
        List<Row> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(filename), schema))) {
            while (reader.hasNext()) {
                results.add(new Row(reader.next()));
            }
        }
        return results;
    }

    public static List<Row> readDataFile(Schema schema, FileReference file) throws IOException {
        return readDataFile(schema, file.getFilename());
    }
}
