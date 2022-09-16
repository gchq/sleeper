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
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CompactSortedFilesTestData {

    public static List<Record> keyAndTwoValuesEvenLongs() {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put("key", (long) 2 * i);
                    record.put("value1", (long) 2 * i);
                    record.put("value2", 987654321L);
                    return record;
                }).collect(Collectors.toList());
    }

    public static List<Record> keyAndTwoValuesOddLongs() {
        return IntStream.range(0, 100)
                .mapToObj(i -> {
                    Record record = new Record();
                    record.put("key", (long) 2 * i + 1);
                    record.put("value1", 1001L);
                    record.put("value2", 123456789L);
                    return record;
                }).collect(Collectors.toList());
    }

    public static List<Record> combineSortedBySingleKey(List<Record> data1, List<Record> data2) {
        SortedMap<Object, Record> data = new TreeMap<>();
        data1.forEach(record -> data.put(record.get("key"), record));
        data2.forEach(record -> data.put(record.get("key"), record));
        return new ArrayList<>(data.values());
    }

    public static void writeData(Schema schema, String filename, List<Record> records) throws IOException {
        try (ParquetRecordWriter writer = new ParquetRecordWriter(new Path(filename), SchemaConverter.getSchema(schema), schema)) {
            for (Record record : records) {
                writer.write(record);
            }
        }
    }
}
