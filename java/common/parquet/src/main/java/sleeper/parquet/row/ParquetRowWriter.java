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
package sleeper.parquet.row;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import sleeper.core.row.Row;
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

import java.util.List;
import java.util.Map;

/**
 * Writes rows to a Parquet file.
 */
class ParquetRowWriter {
    private final RecordConsumer recordConsumer;
    private final Schema schema;

    ParquetRowWriter(RecordConsumer recordConsumer, sleeper.core.schema.Schema schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    void write(Row row) {
        recordConsumer.startMessage();
        int count = 0;
        for (Field entry : schema.getAllFields()) {
            String name = entry.getName();
            Type type = entry.getType();
            recordConsumer.startField(name, count);
            if (type instanceof IntType) {
                recordConsumer.addInteger((int) row.get(name));
            } else if (type instanceof LongType) {
                recordConsumer.addLong((long) row.get(name));
            } else if (type instanceof StringType) {
                recordConsumer.addBinary(Binary.fromString((String) row.get(name)));
            } else if (type instanceof ByteArrayType) {
                recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) row.get(name)));
            } else if (type instanceof MapType) {
                addMap(recordConsumer, (MapType) type, (Map<?, ?>) row.get(name));
            } else if (type instanceof ListType) {
                addList(recordConsumer, (ListType) type, (List<?>) row.get(name));
            } else {
                throw new RuntimeException("Unknown type " + type);
            }
            recordConsumer.endField(name, count);
            count++;
        }
        recordConsumer.endMessage();
    }

    private void addList(RecordConsumer recordConsumer, ListType listType, List<?> list) {
        PrimitiveType elementType = listType.getElementType();
        recordConsumer.startGroup();
        if (!list.isEmpty()) {
            recordConsumer.startField("list", 0);
            recordConsumer.startGroup();
            recordConsumer.startField("element", 0);
            if (elementType instanceof IntType) {
                for (Object o : list) {
                    recordConsumer.addInteger((int) o);
                }
            } else if (elementType instanceof LongType) {
                for (Object o : list) {
                    recordConsumer.addLong((long) o);
                }
            } else if (elementType instanceof StringType) {
                for (Object o : list) {
                    recordConsumer.addBinary(Binary.fromString((String) o));
                }
            } else if (elementType instanceof ByteArrayType) {
                for (Object o : list) {
                    recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) o));
                }
            } else {
                throw new RuntimeException("Unknown type" + elementType);
            }
            recordConsumer.endField("element", 0);
            recordConsumer.endGroup();
            recordConsumer.endField("list", 0);
        }
        recordConsumer.endGroup();
    }

    private void addMap(RecordConsumer recordConsumer, MapType mapType, Map<?, ?> map) {
        PrimitiveType keyType = mapType.getKeyType();
        PrimitiveType valueType = mapType.getValueType();
        recordConsumer.startGroup();
        if (!map.isEmpty()) {
            recordConsumer.startField("key_value", 0);
            recordConsumer.startGroup();
            recordConsumer.startField("key", 0);
            if (keyType instanceof IntType) {
                for (Object key : map.keySet()) {
                    recordConsumer.addInteger((int) key);
                }
            } else if (keyType instanceof LongType) {
                for (Object key : map.keySet()) {
                    recordConsumer.addLong((long) key);
                }
            } else if (keyType instanceof StringType) {
                for (Object key : map.keySet()) {
                    recordConsumer.addBinary(Binary.fromString((String) key));
                }
            } else if (keyType instanceof ByteArrayType) {
                for (Object key : map.keySet()) {
                    recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) key));
                }
            } else {
                throw new RuntimeException("Unknown type " + keyType);
            }
            recordConsumer.endField("key", 0);
            recordConsumer.startField("value", 1);
            if (valueType instanceof IntType) {
                for (Object value : map.values()) {
                    recordConsumer.addInteger((int) value);
                }
            } else if (valueType instanceof LongType) {
                for (Object value : map.values()) {
                    recordConsumer.addLong((long) value);
                }
            } else if (valueType instanceof StringType) {
                for (Object value : map.values()) {
                    recordConsumer.addBinary(Binary.fromString((String) value));
                }
            } else if (valueType instanceof ByteArrayType) {
                for (Object value : map.values()) {
                    recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) value));
                }
            } else {
                throw new RuntimeException("Unknown type " + valueType);
            }
            recordConsumer.endField("value", 1);
            recordConsumer.endGroup();
            recordConsumer.endField("key_value", 0);
        }
        recordConsumer.endGroup();
    }
}
