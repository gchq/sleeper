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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.UInt8Writer;
import org.apache.arrow.vector.complex.writer.VarBinaryWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Helpers for Arrow data.
 */
public class ArrowFormatUtils {

    private ArrowFormatUtils() {
    }

    /**
     * Write a string value to an Arrow struct.
     *
     * @param struct    the struct writer
     * @param allocator the allocator
     * @param field     the field
     * @param value     the value
     */
    public static void writeVarChar(StructWriter struct, BufferAllocator allocator, Field field, String value) {
        writeVarChar(struct.varChar(field.getName()), allocator, value);
    }

    /**
     * Write a nullable string value to an Arrow VarChar field.
     *
     * @param writer    the writer
     * @param allocator the allocator
     * @param value     the value
     */
    public static void writeVarCharNullable(VarCharWriter writer, BufferAllocator allocator, String value) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        writeVarChar(writer, allocator, value);
    }

    /**
     * Write a string value to an Arrow VarChar field.
     *
     * @param writer    the writer
     * @param allocator the allocator
     * @param value     the value
     */
    public static void writeVarChar(VarCharWriter writer, BufferAllocator allocator, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        try (ArrowBuf buffer = allocator.buffer(bytes.length)) {
            buffer.setBytes(0, bytes);
            writer.writeVarChar(0, bytes.length, buffer);
        }
    }

    /**
     * Write a nullable string value to an Arrow struct.
     *
     * @param struct    the struct writer
     * @param allocator the allocator
     * @param field     the field
     * @param value     the value
     */
    public static void writeVarCharNullable(StructWriter struct, BufferAllocator allocator, Field field, String value) {
        writeVarCharNullable(struct.varChar(field.getName()), allocator, value);
    }

    /**
     * Write a timestamp in milliseconds to an Arrow struct.
     *
     * @param struct the struct writer
     * @param field  the field
     * @param value  the value
     */
    public static void writeTimeStampMilli(StructWriter struct, Field field, Instant value) {
        struct.timeStampMilli(field.getName()).writeTimeStampMilli(value.toEpochMilli());
    }

    /**
     * Write a long value to an Arrow struct.
     *
     * @param struct the struct writer
     * @param field  the field
     * @param value  the value
     */
    public static void writeUInt8(StructWriter struct, Field field, long value) {
        struct.uInt8(field.getName()).writeUInt8(value);
    }

    /**
     * Write a nullable long value to an Arrow struct.
     *
     * @param struct the struct writer
     * @param field  the field
     * @param value  the value
     */
    public static void writeUInt8Nullable(StructWriter struct, Field field, Long value) {
        writeUInt8Nullable(struct.uInt8(field.getName()), value);
    }

    /**
     * Write a nullable long value to an UInt8 field.
     *
     * @param writer the writer
     * @param value  the value
     */
    public static void writeUInt8Nullable(UInt8Writer writer, Long value) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        writer.writeUInt8(value);
    }

    /**
     * Write a nullable long value to a BigInt field.
     *
     * @param writer the writer
     * @param value  the value
     */
    public static void writeBigIntNullable(BigIntWriter writer, Long value) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        writer.writeBigInt(value);
    }

    /**
     * Write a nullable int value to an Int field.
     *
     * @param writer the writer
     * @param value  the value
     */
    public static void writeIntNullable(IntWriter writer, Integer value) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        writer.writeInt(value);
    }

    /**
     * Write a nullable byte array value to a VarBinary field.
     *
     * @param writer the writer
     * @param value  the value
     */
    public static void writeVarBinaryNullable(VarBinaryWriter writer, BufferAllocator allocator, byte[] value) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        try (ArrowBuf buffer = allocator.buffer(value.length)) {
            buffer.setBytes(0, value);
            writer.writeVarBinary(0, value.length, buffer);
        }
    }

    /**
     * Write a boolean value to an Arrow struct.
     *
     * @param struct the struct writer
     * @param field  the field
     * @param value  the value
     */
    public static void writeBit(StructWriter struct, Field field, boolean value) {
        struct.bit(field.getName()).writeBit(value ? 1 : 0);
    }
}
