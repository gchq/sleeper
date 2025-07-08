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

import sleeper.core.record.SleeperRow;
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

    public static List<SleeperRow> keyAndTwoValuesSortedEvenLongs() {
        return streamKeyAndTwoValuesFromEvens(n -> (long) n)
                .collect(Collectors.toList());
    }

    public static List<SleeperRow> keyAndTwoValuesSortedOddLongs() {
        return streamKeyAndTwoValuesFromOdds(n -> (long) n)
                .collect(Collectors.toList());
    }

    public static List<SleeperRow> keyAndTwoValuesSortedEvenStrings() {
        return streamKeyAndTwoValuesFromEvens(CompactionRunnerTestData::nthString)
                .collect(Collectors.toList());
    }

    public static List<SleeperRow> keyAndTwoValuesSortedOddStrings() {
        return streamKeyAndTwoValuesFromOdds(CompactionRunnerTestData::nthString)
                .collect(Collectors.toList());
    }

    public static List<SleeperRow> keyAndTwoValuesSortedEvenByteArrays() {
        return streamKeyAndTwoValuesFromEvens(CompactionRunnerTestData::nthByteArray)
                .collect(Collectors.toList());
    }

    public static List<SleeperRow> keyAndTwoValuesSortedOddByteArrays() {
        return streamKeyAndTwoValuesFromOdds(CompactionRunnerTestData::nthByteArray)
                .collect(Collectors.toList());
    }

    private static Stream<SleeperRow> streamKeyAndTwoValuesFromEvens(Function<Integer, Object> convert) {
        return streamFromEvens((even, record) -> {
            Object converted = convert.apply(even);
            record.put("key", converted);
            record.put("value1", converted);
            record.put("value2", 987654321L);
        });
    }

    private static Stream<SleeperRow> streamKeyAndTwoValuesFromOdds(Function<Integer, Object> convert) {
        Object value1 = convert.apply(1001);
        return streamFromOdds((odd, record) -> {
            record.put("key", convert.apply(odd));
            record.put("value1", value1);
            record.put("value2", 123456789L);
        });
    }

    public static List<SleeperRow> specifiedFromEvens(BiConsumer<Integer, SleeperRow> setRecord) {
        return streamFromEvens(setRecord).collect(Collectors.toList());
    }

    public static List<SleeperRow> specifiedFromOdds(BiConsumer<Integer, SleeperRow> setRecord) {
        return streamFromOdds(setRecord).collect(Collectors.toList());
    }

    private static Stream<SleeperRow> streamFromEvens(BiConsumer<Integer, SleeperRow> setRecord) {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    int even = 2 * i;
                    SleeperRow record = new SleeperRow();
                    setRecord.accept(even, record);
                    return record;
                });
    }

    private static Stream<SleeperRow> streamFromOdds(BiConsumer<Integer, SleeperRow> setRecord) {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    int odd = 2 * i + 1;
                    SleeperRow record = new SleeperRow();
                    setRecord.accept(odd, record);
                    return record;
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

    public static List<SleeperRow> combineSortedBySingleKey(List<SleeperRow> data1, List<SleeperRow> data2) {
        return combineSortedBySingleKey(data1, data2, record -> record.get("key"));
    }

    public static List<SleeperRow> combineSortedBySingleByteArrayKey(List<SleeperRow> data1, List<SleeperRow> data2) {
        return combineSortedBySingleKey(data1, data2, record -> ByteArray.wrap((byte[]) record.get("key")));
    }

    public static List<SleeperRow> combineSortedBySingleKey(List<SleeperRow> data1, List<SleeperRow> data2, Function<SleeperRow, Object> getKey) {
        SortedMap<Object, SleeperRow> data = new TreeMap<>();
        data1.forEach(record -> data.put(getKey.apply(record), record));
        data2.forEach(record -> data.put(getKey.apply(record), record));
        return new ArrayList<>(data.values());
    }

    public static FileReference writeRootFile(Schema schema, StateStore stateStore, String filename, List<SleeperRow> records) throws Exception {
        Sketches sketches = Sketches.from(schema);
        try (ParquetWriter<SleeperRow> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), schema)) {
            for (SleeperRow record : records) {
                writer.write(record);
                sketches.update(record);
            }
        }
        new LocalFileSystemSketchesStore().saveFileSketches(filename, schema, sketches);
        FileReference fileReference = FileReferenceFactory.from(stateStore).rootFile(filename, records.size());
        update(stateStore).addFile(fileReference);
        return fileReference;
    }

    public static List<SleeperRow> readDataFile(Schema schema, String filename) throws IOException {
        List<SleeperRow> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(filename), schema))) {
            while (reader.hasNext()) {
                results.add(new SleeperRow(reader.next()));
            }
        }
        return results;
    }

    public static List<SleeperRow> readDataFile(Schema schema, FileReference file) throws IOException {
        return readDataFile(schema, file.getFilename());
    }
}
