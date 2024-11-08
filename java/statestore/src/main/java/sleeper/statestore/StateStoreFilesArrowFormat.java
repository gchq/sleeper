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
package sleeper.statestore;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.StateStoreFile;
import sleeper.core.statestore.transactionlog.StateStoreFiles;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static sleeper.statestore.ArrowFormatUtils.writeBit;
import static sleeper.statestore.ArrowFormatUtils.writeTimeStampMilli;
import static sleeper.statestore.ArrowFormatUtils.writeUInt8;
import static sleeper.statestore.ArrowFormatUtils.writeVarChar;
import static sleeper.statestore.ArrowFormatUtils.writeVarCharNullable;

/**
 * Reads and writes the state of files in a state store to an Arrow file.
 */
public class StateStoreFilesArrowFormat {

    private static final Field FILENAME = Field.notNullable("filename", Utf8.INSTANCE);
    private static final Field UPDATE_TIME = Field.notNullable("updateTime", Types.MinorType.TIMESTAMPMILLI.getType());
    private static final Field PARTITION_ID = Field.notNullable("partitionId", Utf8.INSTANCE);
    private static final Field REFERENCE_UPDATE_TIME = Field.notNullable("updateTime", Types.MinorType.TIMESTAMPMILLI.getType());
    private static final Field JOB_ID = Field.nullable("jobId", Utf8.INSTANCE);
    private static final Field NUMBER_OF_RECORDS = Field.notNullable("numberOfRecords", Types.MinorType.UINT8.getType());
    private static final Field COUNT_APPROXIMATE = Field.notNullable("countApproximate", Types.MinorType.BIT.getType());
    private static final Field ONLY_CONTAINS_DATA_FOR_THIS_PARTITION = Field.notNullable("onlyContainsDataForThisPartition", Types.MinorType.BIT.getType());
    private static final Field REFERENCE = new Field("reference",
            FieldType.notNullable(Types.MinorType.STRUCT.getType()),
            List.of(PARTITION_ID, REFERENCE_UPDATE_TIME, JOB_ID,
                    NUMBER_OF_RECORDS, COUNT_APPROXIMATE, ONLY_CONTAINS_DATA_FOR_THIS_PARTITION));
    private static final Field REFERENCES = new Field("partitionReferences",
            FieldType.notNullable(Types.MinorType.LIST.getType()), List.of(REFERENCE));
    private static final Schema SCHEMA = new Schema(List.of(FILENAME, UPDATE_TIME, REFERENCES));

    private StateStoreFilesArrowFormat() {
    }

    /**
     * Writes the state of files in Arrow format.
     *
     * @param  files       the files in the state store
     * @param  allocator   the buffer allocator
     * @param  channel     the channel to write to
     * @throws IOException if writing to the channel fails
     */
    public static void write(StateStoreFiles files, BufferAllocator allocator, WritableByteChannel channel) throws IOException {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(SCHEMA, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, channel)) {
            vectorSchemaRoot.getFieldVectors().forEach(fieldVector -> fieldVector.setInitialCapacity(files.referencedAndUnreferenced().size()));
            vectorSchemaRoot.allocateNew();
            writer.start();
            int rowNumber = 0;
            VarCharVector filenameVector = (VarCharVector) vectorSchemaRoot.getVector(FILENAME);
            TimeStampMilliVector updateTimeVector = (TimeStampMilliVector) vectorSchemaRoot.getVector(UPDATE_TIME);
            ListVector referencesVector = (ListVector) vectorSchemaRoot.getVector(REFERENCES);
            for (StateStoreFile file : files.referencedAndUnreferenced()) {
                filenameVector.setSafe(rowNumber, file.getFilename().getBytes(StandardCharsets.UTF_8));
                updateTimeVector.setSafe(rowNumber, file.getLastStateStoreUpdateTime().toEpochMilli());
                writeReferences(file.getReferences(), rowNumber, allocator, referencesVector.getWriter());
                rowNumber++;
                vectorSchemaRoot.setRowCount(rowNumber);
            }
            writer.writeBatch();
            writer.end();
        }
    }

    /**
     * Reads the state of files from Arrow format.
     *
     * @param  channel the channel to read from
     * @return         the files in the state store
     */
    public static StateStoreFiles read(BufferAllocator allocator, ReadableByteChannel channel) throws IOException {
        StateStoreFiles files = new StateStoreFiles();
        try (ArrowStreamReader reader = new ArrowStreamReader(channel, allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
            VarCharVector filenameVector = (VarCharVector) vectorSchemaRoot.getVector(FILENAME);
            TimeStampMilliVector updateTimeVector = (TimeStampMilliVector) vectorSchemaRoot.getVector(UPDATE_TIME);
            ListVector referencesVector = (ListVector) vectorSchemaRoot.getVector(REFERENCES);
            for (int rowNumber = 0; rowNumber < vectorSchemaRoot.getRowCount(); rowNumber++) {
                String filename = filenameVector.getObject(rowNumber).toString();
                List<FileReference> references = readReferences(filename, referencesVector, rowNumber);
                files.add(new StateStoreFile(filename, Instant.ofEpochMilli(updateTimeVector.get(rowNumber)), references));
            }
        }
        return files;
    }

    private static void writeReferences(Collection<FileReference> references, int fileNumber, BufferAllocator allocator, UnionListWriter writer) {
        writer.setPosition(fileNumber);
        writer.startList();
        for (FileReference reference : references) {
            StructWriter struct = writer.struct();
            struct.start();
            writeVarChar(struct, allocator, PARTITION_ID, reference.getPartitionId());
            writeTimeStampMilli(struct, REFERENCE_UPDATE_TIME, reference.getLastStateStoreUpdateTime());
            writeVarCharNullable(struct, allocator, JOB_ID, reference.getJobId());
            writeUInt8(struct, NUMBER_OF_RECORDS, reference.getNumberOfRecords());
            writeBit(struct, COUNT_APPROXIMATE, reference.isCountApproximate());
            writeBit(struct, ONLY_CONTAINS_DATA_FOR_THIS_PARTITION, reference.onlyContainsDataForThisPartition());
            struct.end();
        }
        writer.endList();
    }

    private static List<FileReference> readReferences(String filename, ListVector referencesVector, int rowNumber) {
        List<FileReference> references = new ArrayList<>();
        UnionListReader listReader = referencesVector.getReader();
        listReader.setPosition(rowNumber);
        FieldReader reader = listReader.reader();
        while (listReader.next()) {
            references.add(FileReference.builder()
                    .filename(filename)
                    .partitionId(reader.reader(PARTITION_ID.getName()).readText().toString())
                    .lastStateStoreUpdateTime(reader.reader(REFERENCE_UPDATE_TIME.getName()).readLocalDateTime().toInstant(ZoneOffset.UTC))
                    .jobId(Optional.ofNullable(reader.reader(JOB_ID.getName()).readText()).map(Text::toString).orElse(null))
                    .numberOfRecords(reader.reader(NUMBER_OF_RECORDS.getName()).readLong())
                    .countApproximate(reader.reader(COUNT_APPROXIMATE.getName()).readBoolean())
                    .onlyContainsDataForThisPartition(reader.reader(ONLY_CONTAINS_DATA_FOR_THIS_PARTITION.getName()).readBoolean())
                    .build());
        }
        return references;
    }
}
