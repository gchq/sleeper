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

package sleeper.ingest.testutils;

import org.apache.parquet.hadoop.ParquetWriter;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.IngestProperties;
import sleeper.statestore.StateStore;

import java.util.ArrayList;
import java.util.List;

public class IngestRecordsTestDataHelper {
    public IngestProperties.Builder defaultPropertiesBuilder(StateStore stateStore,
                                                             Schema sleeperSchema,
                                                             String ingestLocalWorkingDirectory,
                                                             String bucketName) throws ObjectFactoryException {
        return IngestProperties.builder()
                .objectFactory(new ObjectFactory(new InstanceProperties(), null, ""))
                .localDir(ingestLocalWorkingDirectory)
                .maxRecordsToWriteLocally(10L)
                .maxInMemoryBatchSize(1000L)
                .rowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .pageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .compressionCodec("zstd")
                .stateStore(stateStore)
                .schema(sleeperSchema)
                .bucketName(bucketName)
                .ingestPartitionRefreshFrequencyInSecond(120);
    }

    public Schema schemaWithRowKeys(Field... fields) {
        return Schema.builder()
                .rowKeyFields(fields)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    public List<Record> getRecords() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", 3L);
        record2.put("value1", 4L);
        record2.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        return records;
    }

    public List<Record> getLotsOfRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            Record record1 = new Record();
            record1.put("key", 1L - i);
            record1.put("value1", 2L * i);
            record1.put("value2", 3L * i);
            Record record2 = new Record();
            record2.put("key", 2L + i);
            record2.put("value1", 4L * i);
            record2.put("value2", 6L * i);
            records.add(record1);
            records.add(record2);
        }
        return records;
    }

    public List<Record> getRecordsInFirstPartitionOnly() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", 0L);
        record2.put("value1", 4L);
        record2.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        return records;
    }

    public List<Record> getRecordsByteArrayKey() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 1});
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", new byte[]{2, 2});
        record2.put("value1", 2L);
        record2.put("value2", 3L);
        Record record3 = new Record();
        record3.put("key", new byte[]{64, 65});
        record3.put("value1", 4L);
        record3.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        return records;
    }

    public List<Record> getRecords2DimByteArrayKey() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key1", new byte[]{1, 1});
        record1.put("key2", new byte[]{2, 3});
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key1", new byte[]{11, 2});
        record2.put("key2", new byte[]{2, 2});
        record2.put("value1", 2L);
        record2.put("value2", 3L);
        Record record3 = new Record();
        record3.put("key1", new byte[]{64, 65});
        record3.put("key2", new byte[]{67, 68});
        record3.put("value1", 4L);
        record3.put("value2", 6L);
        Record record4 = new Record();
        record4.put("key1", new byte[]{5});
        record4.put("key2", new byte[]{99});
        record4.put("value1", 2L);
        record4.put("value2", 3L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record3); // Add twice so that one file has more entries so we can tell them apart
        records.add(record4);
        return records;
    }

    public List<Record> getUnsortedRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 10; i > 0; i--) {
            Record record1 = new Record();
            record1.put("key", (long) i);
            record1.put("value1", 2L);
            record1.put("value2", 3L);
            records.add(record1);
            Record record2 = new Record();
            record2.put("key", 5L);
            record2.put("value1", 4L);
            record2.put("value2", 6L);
            records.add(record2);
        }
        return records;
    }

    public List<Record> getRecordsForAggregationIteratorTest() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 1});
        record1.put("sort", 2L);
        record1.put("value", 1L);
        Record record2 = new Record();
        record2.put("key", new byte[]{11, 2});
        record2.put("sort", 1L);
        record2.put("value", 1L);
        Record record3 = new Record();
        record3.put("key", new byte[]{1, 1});
        record3.put("sort", 2L);
        record3.put("value", 6L);
        Record record4 = new Record();
        record4.put("key", new byte[]{11, 2});
        record4.put("sort", 1L);
        record4.put("value", 3L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        return records;
    }

    public List<Record> getRecordsOscillatingBetween2Partitions() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key1", 0);
        record1.put("key2", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 1L);
        Record record2 = new Record();
        record2.put("key1", 0);
        record2.put("key2", 20L);
        record2.put("value1", 200L);
        record2.put("value2", 100L);
        Record record3 = new Record();
        record3.put("key1", 100);
        record3.put("key2", 1L);
        record3.put("value1", 20000L);
        record3.put("value2", 10000L);
        Record record4 = new Record();
        record4.put("key1", 100);
        record4.put("key2", 50L);
        record4.put("value1", 2000000L);
        record4.put("value2", 1000000L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        return records;
    }
}
