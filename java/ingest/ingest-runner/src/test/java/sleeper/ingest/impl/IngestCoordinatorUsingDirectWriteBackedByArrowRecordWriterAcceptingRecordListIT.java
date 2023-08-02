/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.ingest.impl;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriterAcceptingRecords;
import sleeper.ingest.testutils.IngestTestHelper;
import sleeper.ingest.testutils.RecordGenerator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IngestCoordinatorUsingDirectWriteBackedByArrowRecordWriterAcceptingRecordListIT {
    @TempDir
    public Path temporaryFolder;

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        test(recordListAndSchema, arrow -> arrow
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(16 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(128 * 1024 * 1024L)
        ).ingestAndVerify();
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        test(recordListAndSchema, arrow -> arrow
                .workingBufferAllocatorBytes(16 * 1024 * 1024L)
                .batchBufferAllocatorBytes(16 * 1024 * 1024L)
                .maxNoOfBytesToWriteLocally(2 * 1024 * 1024L)
        ).ingestAndVerify();
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        IngestTestHelper<RecordList> test = test(recordListAndSchema, arrow -> arrow
                .workingBufferAllocatorBytes(32 * 1024L)
                .batchBufferAllocatorBytes(32 * 1024L)
                .maxNoOfBytesToWriteLocally(64 * 1024 * 1024L));
        assertThatThrownBy(test::ingestAndVerify)
                .isInstanceOf(OutOfMemoryException.class)
                .hasNoSuppressedExceptions();
    }

    private IngestTestHelper<RecordList> test(RecordGenerator.RecordListAndSchema recordListAndSchema,
                                              Consumer<ArrowRecordBatchFactory.Builder<RecordList>> arrowConfig) throws Exception {
        return IngestTestHelper.from(temporaryFolder,
                        new Configuration(),
                        recordListAndSchema)
                .stateStoreInMemory(partitions -> partitions.treeWithSingleSplitPoint(0L))
                .directWrite()
                .backedByArrowWithRecordWriter(new ArrowRecordWriterAcceptingRecordList(), arrowConfig)
                .toWrite(buildScrambledRecordLists(recordListAndSchema));
    }

    private List<RecordList> buildScrambledRecordLists(RecordGenerator.RecordListAndSchema recordListAndSchema) {
        RecordList[] recordLists = new RecordList[5];
        for (int i = 0; i < recordLists.length; i++) {
            recordLists[i] = new RecordList();
        }
        int i = 0;
        for (Record record : recordListAndSchema.recordList) {
            recordLists[i].addRecord(record);
            i++;
            if (i == 5) {
                i = 0;
            }
        }
        return List.of(recordLists);
    }

    static class RecordList {
        private final List<Record> records;

        RecordList() {
            this.records = new ArrayList<>();
        }

        public void addRecord(Record record) {
            records.add(record);
        }

        public List<Record> getRecords() {
            return records;
        }
    }

    static class ArrowRecordWriterAcceptingRecordList implements ArrowRecordWriter<RecordList> {

        @Override
        public int insert(List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, RecordList recordList, int startInsertAtRowNo) {
            int i = 0;
            for (Record record : recordList.getRecords()) {
                ArrowRecordWriterAcceptingRecords.writeRecord(
                        allFields, vectorSchemaRoot, record, startInsertAtRowNo + i);
                i++;
            }
            int finalRowCount = startInsertAtRowNo + i;
            vectorSchemaRoot.setRowCount(finalRowCount);
            return finalRowCount;
        }
    }
}
