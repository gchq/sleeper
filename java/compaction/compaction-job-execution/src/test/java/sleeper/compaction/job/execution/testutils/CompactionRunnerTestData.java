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
package sleeper.compaction.job.execution.testutils;

import com.facebook.collections.ByteArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

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

import static sleeper.sketches.s3.SketchesSerDeToS3.sketchesPathForDataFile;

public class CompactionRunnerTestData {

    private CompactionRunnerTestData() {
    }

    public static List<Record> keyAndTwoValuesSortedEvenLongs() {
        return streamKeyAndTwoValuesFromEvens(n -> (long) n)
                .collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedOddLongs() {
        return streamKeyAndTwoValuesFromOdds(n -> (long) n)
                .collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedEvenStrings() {
        return streamKeyAndTwoValuesFromEvens(CompactionRunnerTestData::nthString)
                .collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedOddStrings() {
        return streamKeyAndTwoValuesFromOdds(CompactionRunnerTestData::nthString)
                .collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedEvenByteArrays() {
        return streamKeyAndTwoValuesFromEvens(CompactionRunnerTestData::nthByteArray)
                .collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedOddByteArrays() {
        return streamKeyAndTwoValuesFromOdds(CompactionRunnerTestData::nthByteArray)
                .collect(Collectors.toList());
    }

    private static Stream<Record> streamKeyAndTwoValuesFromEvens(Function<Integer, Object> convert) {
        return streamFromEvens((even, record) -> {
            Object converted = convert.apply(even);
            record.put("key", converted);
            record.put("value1", converted);
            record.put("value2", 987654321L);
        });
    }

    private static Stream<Record> streamKeyAndTwoValuesFromOdds(Function<Integer, Object> convert) {
        Object value1 = convert.apply(1001);
        return streamFromOdds((odd, record) -> {
            record.put("key", convert.apply(odd));
            record.put("value1", value1);
            record.put("value2", 123456789L);
        });
    }

    public static List<Record> specifiedFromEvens(BiConsumer<Integer, Record> setRecord) {
        return streamFromEvens(setRecord).collect(Collectors.toList());
    }

    public static List<Record> specifiedFromOdds(BiConsumer<Integer, Record> setRecord) {
        return streamFromOdds(setRecord).collect(Collectors.toList());
    }

    private static Stream<Record> streamFromEvens(BiConsumer<Integer, Record> setRecord) {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    int even = 2 * i;
                    Record record = new Record();
                    setRecord.accept(even, record);
                    return record;
                });
    }

    private static Stream<Record> streamFromOdds(BiConsumer<Integer, Record> setRecord) {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    int odd = 2 * i + 1;
                    Record record = new Record();
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

    public static List<Record> combineSortedBySingleKey(List<Record> data1, List<Record> data2) {
        return combineSortedBySingleKey(data1, data2, record -> record.get("key"));
    }

    public static List<Record> combineSortedBySingleByteArrayKey(List<Record> data1, List<Record> data2) {
        return combineSortedBySingleKey(data1, data2, record -> ByteArray.wrap((byte[]) record.get("key")));
    }

    public static List<Record> combineSortedBySingleKey(List<Record> data1, List<Record> data2, Function<Record, Object> getKey) {
        SortedMap<Object, Record> data = new TreeMap<>();
        data1.forEach(record -> data.put(getKey.apply(record), record));
        data2.forEach(record -> data.put(getKey.apply(record), record));
        return new ArrayList<>(data.values());
    }

    public static FileReference writeRootFile(Schema schema, StateStore stateStore, String filename, List<Record> records) throws Exception {
        Sketches sketches = Sketches.from(schema);
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), schema)) {
            for (Record record : records) {
                writer.write(record);
                sketches.update(record);
            }
        }
        Path sketchesPath = sketchesPathForDataFile(filename);
        new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPath, sketches, new Configuration());
        FileReference fileReference = FileReferenceFactory.from(stateStore).rootFile(filename, records.size());
        stateStore.addFile(fileReference);
        return fileReference;
    }

    public static List<Record> readDataFile(Schema schema, String filename) throws IOException {
        List<Record> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(filename), schema))) {
            while (reader.hasNext()) {
                results.add(new Record(reader.next()));
            }
        }
        return results;
    }

    public static List<Record> readDataFile(Schema schema, FileReference file) throws IOException {
        return readDataFile(schema, file.getFilename());
    }
}
