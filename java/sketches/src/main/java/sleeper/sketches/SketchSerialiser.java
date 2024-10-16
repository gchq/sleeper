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
package sleeper.sketches;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.ArrayOfNumbersSerDe;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class SketchSerialiser {
    private final Schema schema;

    public SketchSerialiser(Schema schema) {
        this.schema = schema;
    }

    public void serialise(Sketches sketches, DataOutputStream dos) throws IOException {
        for (Field field : schema.getRowKeyFields()) {
            if (field.getType() instanceof IntType || field.getType() instanceof LongType) {
                ItemsSketch<Number> sketch = sketches.getQuantilesSketch(field.getName());
                byte[] b = sketch.toByteArray(new ArrayOfNumbersSerDe());
                dos.writeInt(b.length);
                dos.write(b);
            } else if (field.getType() instanceof StringType) {
                ItemsSketch<String> sketch = sketches.getQuantilesSketch(field.getName());
                byte[] b = sketch.toByteArray(new ArrayOfStringsSerDe());
                dos.writeInt(b.length);
                dos.write(b);
            } else if (field.getType() instanceof ByteArrayType) {
                ItemsSketch<ByteArray> sketch = sketches.getQuantilesSketch(field.getName());
                byte[] b = sketch.toByteArray(new ArrayOfByteArraysSerSe());
                dos.writeInt(b.length);
                dos.write(b);
            } else {
                throw new IOException("Unknown key type of " + field.getType());
            }
        }
    }

    public Sketches deserialise(DataInputStream dis) throws IOException {
        Map<String, ItemsSketch> keyFieldToQuantilesSketch = new HashMap<>();
        for (Field field : schema.getRowKeyFields()) {
            if (field.getType() instanceof IntType || field.getType() instanceof LongType) {
                int length = dis.readInt();
                byte[] b = new byte[length];
                dis.readFully(b);
                ItemsSketch<Object> sketch = ItemsSketch.getInstance(WritableMemory.writableWrap(b), Comparator.naturalOrder(), (ArrayOfItemsSerDe) new ArrayOfNumbersSerDe());
                keyFieldToQuantilesSketch.put(field.getName(), sketch);
            } else if (field.getType() instanceof StringType) {
                int length = dis.readInt();
                byte[] b = new byte[length];
                dis.readFully(b);
                ItemsSketch<String> sketch = ItemsSketch.getInstance(Memory.wrap(b), Comparator.naturalOrder(), new ArrayOfStringsSerDe());
                keyFieldToQuantilesSketch.put(field.getName(), sketch);
            } else if (field.getType() instanceof ByteArrayType) {
                int length = dis.readInt();
                byte[] b = new byte[length];
                dis.readFully(b);
                ItemsSketch<ByteArray> sketch = ItemsSketch.getInstance(WritableMemory.writableWrap(b), Comparator.naturalOrder(), new ArrayOfByteArraysSerSe());
                keyFieldToQuantilesSketch.put(field.getName(), sketch);
            } else {
                throw new IOException("Unknown key type of " + field.getType());
            }
        }
        return new Sketches(keyFieldToQuantilesSketch);
    }

    /**
     * The following code is heavily based on ArrayOfStringsSerDe from the DataSketches library.
     */
    public static class ArrayOfByteArraysSerSe extends ArrayOfItemsSerDe<ByteArray> {

        @Override
        public byte[] serializeToByteArray(ByteArray[] items) {
            int length = 0;
            byte[][] itemsBytes = new byte[items.length][];
            for (int i = 0; i < items.length; i++) {
                itemsBytes[i] = items[i].getArray();
                length += itemsBytes[i].length + Integer.BYTES;
            }
            byte[] bytes = new byte[length];
            WritableMemory mem = WritableMemory.writableWrap(bytes);
            long offsetBytes = 0;
            for (int i = 0; i < items.length; i++) {
                mem.putInt(offsetBytes, itemsBytes[i].length);
                offsetBytes += Integer.BYTES;
                mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
                offsetBytes += itemsBytes[i].length;
            }
            return bytes;
        }

        @Override
        public ByteArray[] deserializeFromMemory(Memory memory, int numItems) {
            ByteArray[] array = new ByteArray[numItems];
            long offsetBytes = 0;
            for (int i = 0; i < numItems; i++) {
                Util.checkBounds(offsetBytes, Integer.BYTES, memory.getCapacity());
                int byteArrayLength = memory.getInt(offsetBytes);
                offsetBytes += Integer.BYTES;
                byte[] bytes = new byte[byteArrayLength];
                Util.checkBounds(offsetBytes, byteArrayLength, memory.getCapacity());
                memory.getByteArray(offsetBytes, bytes, 0, byteArrayLength);
                offsetBytes += byteArrayLength;
                array[i] = ByteArray.wrap(bytes);
            }
            return array;
        }
    }
}
