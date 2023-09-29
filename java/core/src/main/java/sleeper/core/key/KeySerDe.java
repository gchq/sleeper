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
package sleeper.core.key;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Serialises key. Fewer key than there are row key in the schema may be
 * serialised, but they must be in the same order as in the schema, e.g. if
 * the row key types in the schema are int, string, then (5, "A") can be
 * serialised, as can (5), but ("A") cannot be serialised.
 */
public class KeySerDe {
    private static final String NULL_STRING_MARKER = "SLEEPER-NULL-STRING";
    private static final byte[] NULL_BYTE_ARRAY_MARKER = "SLEEPER-NULL-BYTE-ARRAY".getBytes(Charset.forName("UTF-8"));

    private final List<PrimitiveType> rowKeyTypes;
    private final int numRowKeysInSchema;

    public KeySerDe(Schema schema) {
        this.rowKeyTypes = schema.getRowKeyTypes();
        this.numRowKeysInSchema = this.rowKeyTypes.size();
    }

    public KeySerDe(List<PrimitiveType> rowKeyTypes) {
        this.rowKeyTypes = new ArrayList<>();
        this.rowKeyTypes.addAll(rowKeyTypes);
        this.numRowKeysInSchema = this.rowKeyTypes.size();
    }

    public byte[] serialise(Key key) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        int numKeys = key != null ? key.size() : 0;
        int numKeysToSerialise = Math.min(numKeys, numRowKeysInSchema);
        dos.writeInt(numKeysToSerialise);
        for (int i = 0; i < numKeysToSerialise; i++) {
            PrimitiveType type = rowKeyTypes.get(i);
            if (type instanceof IntType) {
                if (null == key.get(i)) {
                    // A partition can have a maximum value of null to indicate
                    // that any int is less than it (the partition consisting
                    // of all integers needs to have null as the max because
                    // the max of a partition is not contained within the
                    // partition and the root partition for an int key needs
                    // to contain all ints, hence we cannot use Integer.MAX_VALUE
                    // as the maximum).
                    dos.writeBoolean(true);
                } else {
                    dos.writeBoolean(false);
                    dos.writeInt((int) key.get(i));
                }
            } else if (type instanceof LongType) {
                if (null == key.get(i)) {
                    dos.writeBoolean(true);
                } else {
                    dos.writeBoolean(false);
                    dos.writeLong((long) key.get(i));
                }
            } else if (type instanceof StringType) {
                if (null == key.get(i)) {
                    dos.writeUTF(NULL_STRING_MARKER);
                } else {
                    dos.writeUTF((String) key.get(i));
                }
            } else if (type instanceof ByteArrayType) {
                byte[] bytes;
                if (null == key.get(i)) {
                    bytes = NULL_BYTE_ARRAY_MARKER;
                } else {
                    bytes = (byte[]) key.get(i);
                }
                dos.writeInt(bytes.length);
                dos.write(bytes);
            } else {
                throw new IllegalArgumentException("Unknown type " + type);
            }
        }
        dos.close();
        return baos.toByteArray();
    }

    public Key deserialise(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        int numSerialisedKeys = dis.readInt();
        List<Object> key = new ArrayList<>();
        for (int i = 0; i < numSerialisedKeys; i++) {
            PrimitiveType type = rowKeyTypes.get(i);
            if (type instanceof IntType) {
                if (dis.readBoolean()) {
                    key.add(null);
                } else {
                    key.add(dis.readInt());
                }
            } else if (type instanceof LongType) {
                if (dis.readBoolean()) {
                    key.add(null);
                } else {
                    key.add(dis.readLong());
                }
            } else if (type instanceof StringType) {
                String s = dis.readUTF();
                if (NULL_STRING_MARKER.equals(s)) {
                    key.add(null);
                } else {
                    key.add(s);
                }
            } else if (type instanceof ByteArrayType) {
                int length = dis.readInt();
                byte[] byteArray = new byte[length];
                dis.readFully(byteArray);
                if (Arrays.equals(NULL_BYTE_ARRAY_MARKER, byteArray)) {
                    key.add(null);
                } else {
                    key.add(byteArray);
                }
            } else {
                throw new IllegalArgumentException("Unknown type " + type);
            }
        }
        dis.close();
        if (key.isEmpty()) {
            return null;
        }
        return Key.create(key);
    }
}
