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
package sleeper.compaction.jobexecution;

import org.apache.hadoop.fs.Path;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CompactSortedFilesTestData {

    private CompactSortedFilesTestData() {
    }

    public static List<Record> keyAndTwoValuesSortedEvenLongs() {
        return streamKeyAndTwoValuesSortedEvenLongs().collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedOddLongs() {
        return streamKeyAndTwoValuesSortedOddLongs().collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesSortedEvenStrings() {
        return toStrings(streamKeyAndTwoValuesSortedEvenLongs());
    }

    public static List<Record> keyAndTwoValuesSortedOddStrings() {
        return toStrings(streamKeyAndTwoValuesSortedOddLongs());
    }

    public static List<Record> toStrings(Stream<Record> records) {
        SortedMap<Object, Record> map = new TreeMap<>();
        records.map(CompactSortedFilesTestData::toStrings)
                .forEach(record -> map.put(record.get("key"), record));
        return new ArrayList<>(map.values());
    }

    public static Record toStrings(Record record) {
        record.put("key", "" + record.get("key"));
        record.put("value1", "" + record.get("value1"));
        return record;
    }

    public static Stream<Record> streamKeyAndTwoValuesSortedEvenLongs() {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put("key", (long) 2 * i);
                    record.put("value1", (long) 2 * i);
                    record.put("value2", 987654321L);
                    return record;
                });
    }

    public static Stream<Record> streamKeyAndTwoValuesSortedOddLongs() {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put("key", (long) 2 * i + 1);
                    record.put("value1", 1001L);
                    record.put("value2", 123456789L);
                    return record;
                });
    }

    public static List<Record> combineSortedBySingleKey(List<Record> data1, List<Record> data2) {
        SortedMap<Object, Record> data = new TreeMap<>();
        data1.forEach(record -> data.put(record.get("key"), record));
        data2.forEach(record -> data.put(record.get("key"), record));
        return new ArrayList<>(data.values());
    }

    public static void writeDataFile(Schema schema, String filename, List<Record> records) throws IOException {
        try (ParquetRecordWriter writer = new ParquetRecordWriter(new Path(filename), SchemaConverter.getSchema(schema), schema)) {
            for (Record record : records) {
                writer.write(record);
            }
        }
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
}
