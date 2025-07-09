/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.row.serialiser;

import sleeper.core.row.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialises and deserialises a row to and from a byte array.
 */
public class RowSerialiser {
    private final Schema schema;

    public RowSerialiser(Schema schema) {
        this.schema = schema;
    }

    /**
     * Serialises a row to a byte array.
     *
     * @param  record      the row to serialise
     * @return             a byte array representing the row
     * @throws IOException if a field type is unknown
     */
    public byte[] serialise(Record row) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        for (Field field : schema.getAllFields()) {
            Object value = row.get(field.getName());
            Type type = field.getType();
            if (type instanceof PrimitiveType) {
                write(value, (PrimitiveType) type, dos);
            } else if (type instanceof MapType) {
                MapType mapType = (MapType) type;
                PrimitiveType keyType = mapType.getKeyType();
                PrimitiveType valueType = mapType.getValueType();
                Map<?, ?> map = (Map<?, ?>) value;
                dos.writeInt(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    write(entry.getKey(), keyType, dos);
                    write(entry.getValue(), valueType, dos);
                }
            } else if (type instanceof ListType) {
                ListType listType = (ListType) type;
                PrimitiveType elementType = listType.getElementType();
                List<?> list = (List<?>) value;
                dos.writeInt(list.size());
                for (Object object : list) {
                    write(object, elementType, dos);
                }
            } else {
                throw new IOException("Unknown type " + type);
            }
        }
        dos.close();
        return baos.toByteArray();
    }

    /**
     * Deserialises a byte array to a row.
     *
     * @param  serialised  a byte array representing the row
     * @return             the deserialised row
     * @throws IOException if a field type is unknown
     */
    public Record deserialise(byte[] serialised) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(serialised);
        DataInputStream dis = new DataInputStream(bais);
        Record row = new Record();
        for (Field field : schema.getAllFields()) {
            Type type = field.getType();
            if (type instanceof PrimitiveType) {
                row.put(field.getName(), read((PrimitiveType) type, dis));
            } else if (type instanceof MapType) {
                MapType mapType = (MapType) type;
                PrimitiveType keyType = mapType.getKeyType();
                PrimitiveType valueType = mapType.getValueType();
                int numEntries = dis.readInt();
                Map<Object, Object> map = new HashMap<>(numEntries);
                for (int i = 0; i < numEntries; i++) {
                    Object key = read(keyType, dis);
                    Object value = read(valueType, dis);
                    map.put(key, value);
                }
                row.put(field.getName(), map);
            } else if (type instanceof ListType) {
                ListType listType = (ListType) type;
                PrimitiveType elementType = listType.getElementType();
                int numEntries = dis.readInt();
                List<Object> list = new ArrayList<>(numEntries);
                for (int i = 0; i < numEntries; i++) {
                    Object object = read(elementType, dis);
                    list.add(object);
                }
                row.put(field.getName(), list);
            } else {
                throw new IOException("Unknown type " + type);
            }
        }
        dis.close();
        return row;
    }

    private void write(Object value, PrimitiveType primitiveType, DataOutputStream dos) throws IOException {
        if (primitiveType instanceof IntType) {
            dos.writeInt((int) value);
        } else if (primitiveType instanceof LongType) {
            dos.writeLong((long) value);
        } else if (primitiveType instanceof StringType) {
            dos.writeUTF((String) value);
        } else if (primitiveType instanceof ByteArrayType) {
            byte[] byteArray = (byte[]) value;
            dos.writeInt(byteArray.length);
            dos.write(byteArray);
        } else {
            throw new IOException("Unknown type " + primitiveType);
        }
    }

    private Object read(PrimitiveType primitiveType, DataInputStream dis) throws IOException {
        if (primitiveType instanceof IntType) {
            return dis.readInt();
        }
        if (primitiveType instanceof LongType) {
            return dis.readLong();
        }
        if (primitiveType instanceof StringType) {
            return dis.readUTF();
        }
        if (primitiveType instanceof ByteArrayType) {
            int length = dis.readInt();
            byte[] byteArray = new byte[length];
            dis.readFully(byteArray);
            return byteArray;
        }
        throw new IOException("Unknown type " + primitiveType);
    }
}
