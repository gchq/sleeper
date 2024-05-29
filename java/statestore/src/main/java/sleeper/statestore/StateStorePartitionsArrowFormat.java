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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.UInt4Vector;
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

import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static sleeper.statestore.ArrowFormatUtils.writeVarChar;
import static sleeper.statestore.ArrowFormatUtils.writeVarCharNullable;

/**
 * Reads and writes the state of partitions in a state store to an Arrow file.
 */
public class StateStorePartitionsArrowFormat {
    private static final Field ID = Field.notNullable("id", Utf8.INSTANCE);
    private static final Field PARENT_ID = Field.nullable("parentId", Utf8.INSTANCE);
    private static final Field CHILD_ID = Field.notNullable("childId", Utf8.INSTANCE);
    private static final Field CHILD_IDS = new Field("childIds",
            FieldType.notNullable(Types.MinorType.LIST.getType()), List.of(CHILD_ID));
    private static final Field IS_LEAF = Field.notNullable("isLeaf", Types.MinorType.BIT.getType());
    private static final Field DIMENSION = Field.nullable("dimension", Types.MinorType.UINT4.getType());

    private static final Field FIELD = Field.notNullable("field", Utf8.INSTANCE);
    private static final Field MIN = Field.notNullable("min", Utf8.INSTANCE);
    private static final Field MAX = Field.nullable("max", Utf8.INSTANCE);
    private static final Field RANGE = new Field("range",
            FieldType.notNullable(Types.MinorType.STRUCT.getType()), List.of(FIELD, MIN, MAX));
    private static final Field REGION = new Field("region",
            FieldType.notNullable(Types.MinorType.LIST.getType()), List.of(RANGE));
    private static final Schema SCHEMA = new Schema(List.of(
            ID, PARENT_ID, CHILD_IDS, IS_LEAF, DIMENSION, REGION));

    private StateStorePartitionsArrowFormat() {
    }

    /**
     * Writes the state of partitions in Arrow format.
     *
     * @param  partitions  the partitions in the state store
     * @param  allocator   the buffer allocator
     * @param  channel     the channel to write to
     * @throws IOException if writing to the channel fails
     */
    public static void write(Collection<Partition> partitions, BufferAllocator allocator, WritableByteChannel channel) throws IOException {
        try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(SCHEMA, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, channel)) {
            vectorSchemaRoot.getFieldVectors().forEach(fieldVector -> fieldVector.setInitialCapacity(partitions.size()));
            vectorSchemaRoot.allocateNew();
            writer.start();
            int rowNumber = 0;
            VarCharVector idVector = (VarCharVector) vectorSchemaRoot.getVector(ID);
            VarCharVector parentIdVector = (VarCharVector) vectorSchemaRoot.getVector(PARENT_ID);
            ListVector childIdsVector = (ListVector) vectorSchemaRoot.getVector(CHILD_IDS);
            BitVector isLeafVector = (BitVector) vectorSchemaRoot.getVector(IS_LEAF);
            UInt4Vector dimensionVector = (UInt4Vector) vectorSchemaRoot.getVector(DIMENSION);
            ListVector regionVector = (ListVector) vectorSchemaRoot.getVector(REGION);
            for (Partition partition : partitions) {
                idVector.setSafe(rowNumber, partition.getId().getBytes(StandardCharsets.UTF_8));
                if (partition.getParentPartitionId() != null) {
                    parentIdVector.setSafe(rowNumber, partition.getParentPartitionId().getBytes(StandardCharsets.UTF_8));
                } else {
                    parentIdVector.setNull(rowNumber);
                }
                isLeafVector.setSafe(rowNumber, partition.isLeafPartition() ? 1 : 0);
                if (partition.getDimension() >= 0) {
                    dimensionVector.setSafe(rowNumber, partition.getDimension());
                } else {
                    dimensionVector.setNull(rowNumber);
                }
                writeChildIds(partition, rowNumber, allocator, childIdsVector.getWriter());
                writeRegion(partition.getRegion(), rowNumber, allocator, regionVector.getWriter());

                rowNumber++;
                vectorSchemaRoot.setRowCount(rowNumber);
            }

            writer.writeBatch();
            writer.end();
        }
    }

    /**
     * Reads the state of partitions from Arrow format.
     *
     * @param  channel the channel to read from
     * @return         the partitions in the state store
     */
    public static List<Partition> read(BufferAllocator allocator, ReadableByteChannel channel) throws IOException {
        List<Partition> partitions = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(channel, allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
            VarCharVector idVector = (VarCharVector) vectorSchemaRoot.getVector(ID);
            VarCharVector parentIdVector = (VarCharVector) vectorSchemaRoot.getVector(PARENT_ID);
            ListVector childIdsVector = (ListVector) vectorSchemaRoot.getVector(CHILD_IDS);
            BitVector isLeafVector = (BitVector) vectorSchemaRoot.getVector(IS_LEAF);
            UInt4Vector dimensionVector = (UInt4Vector) vectorSchemaRoot.getVector(DIMENSION);
            ListVector regionVector = (ListVector) vectorSchemaRoot.getVector(REGION);
            for (int rowNumber = 0; rowNumber < vectorSchemaRoot.getRowCount(); rowNumber++) {
                partitions.add(Partition.builder()
                        .id(idVector.getObject(rowNumber).toString())
                        .parentPartitionId(Optional.ofNullable(parentIdVector.getObject(rowNumber)).map(Text::toString).orElse(null))
                        .leafPartition(isLeafVector.getObject(rowNumber))
                        .childPartitionIds(readChildIds(childIdsVector, rowNumber))
                        .dimension(Optional.ofNullable(dimensionVector.getObject(rowNumber)).orElse(-1))
                        .region(readRegion(regionVector, rowNumber))
                        .build());
            }
        }
        return partitions;
    }

    private static void writeChildIds(Partition partition, int rowNumber, BufferAllocator allocator, UnionListWriter writer) {
        writer.setPosition(rowNumber);
        writer.startList();
        for (String childId : partition.getChildPartitionIds()) {
            writeVarChar(writer.varChar(), allocator, childId);
        }
        writer.endList();
    }

    private static void writeRegion(Region region, int rowNumber, BufferAllocator allocator, UnionListWriter writer) {
        writer.setPosition(rowNumber);
        writer.startList();
        for (Range range : region.getRanges()) {
            StructWriter struct = writer.struct();
            struct.start();
            writeVarChar(struct, allocator, FIELD, range.getFieldName());
            writeVarChar(struct, allocator, MIN, (String) range.getMin());
            writeVarCharNullable(struct, allocator, MAX, (String) range.getMax());
            struct.end();
        }
        writer.endList();
    }

    private static List<String> readChildIds(ListVector childIdsVector, int rowNumber) {
        List<String> childIds = new ArrayList<>();
        UnionListReader listReader = childIdsVector.getReader();
        listReader.setPosition(rowNumber);
        FieldReader reader = listReader.reader();
        while (listReader.next()) {
            childIds.add(reader.readText().toString());
        }
        return childIds;
    }

    private static Region readRegion(ListVector regionVector, int rowNumber) {
        List<Range> ranges = new ArrayList<>();
        UnionListReader listReader = regionVector.getReader();
        listReader.setPosition(rowNumber);
        FieldReader reader = listReader.reader();
        while (listReader.next()) {
            ranges.add(new Range(
                    new sleeper.core.schema.Field(reader.reader(FIELD.getName()).readText().toString(), new StringType()),
                    reader.reader(MIN.getName()).readText().toString(),
                    Optional.ofNullable(reader.reader(MAX.getName()).readText()).map(Text::toString).orElse(null)));
        }
        return new Region(ranges);
    }
}
