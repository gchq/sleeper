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

package sleeper.io.parquet.arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import sleeper.arrow.record.RecordBackedByArrow;
import sleeper.arrow.schema.SchemaBackedByArrow;

import java.util.List;
import java.util.Map;

public class ArrowWriter {
    private final RecordConsumer recordConsumer;
    private final SchemaBackedByArrow schemaBackedByArrow;

    public ArrowWriter(RecordConsumer recordConsumer, SchemaBackedByArrow schemaBackedByArrow) {
        this.recordConsumer = recordConsumer;
        this.schemaBackedByArrow = schemaBackedByArrow;
    }

    public void write(RecordBackedByArrow record) {
        recordConsumer.startMessage();
        int count = 0;
        for (Field entry : schemaBackedByArrow.getArrowSchema().getFields()) {
            String name = entry.getName();
            ArrowType type = entry.getType();
            recordConsumer.startField(name, count);
            if (type.equals(new ArrowType.Int(32, true))) {
                recordConsumer.addInteger((int) record.get(name));
            } else if (type.equals(new ArrowType.Int(64, true))) {
                recordConsumer.addLong((long) record.get(name));
            } else if (type instanceof ArrowType.Utf8) {
                recordConsumer.addBinary(Binary.fromString((String) record.get(name)));
            } else if (type instanceof ArrowType.Binary) {
                recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) record.get(name)));
            } else if (type.equals(new ArrowType.List())) {
                if (entry.getChildren().size() == 1) {
                    if (entry.getChildren().get(0).getType() instanceof ArrowType.PrimitiveType) {
                        addList(recordConsumer, (ArrowType.PrimitiveType) entry.getChildren().get(0).getType(), (List<?>) record.get(name));
                    } else if (entry.getChildren().get(0).getType() instanceof ArrowType.Struct
                            && entry.getChildren().get(0).getChildren().size() == 2
                            && entry.getChildren().get(0).getChildren().stream().allMatch(c -> c.getType() instanceof ArrowType.PrimitiveType)) {
                        addMap(recordConsumer,
                                (ArrowType.PrimitiveType) entry.getChildren().get(0).getChildren().get(0).getType(),
                                (ArrowType.PrimitiveType) entry.getChildren().get(0).getChildren().get(1).getType(),
                                (Map<?, ?>) record.get(name));
                    }
                }
            } else {
                throw new RuntimeException("Unknown type " + type);
            }
            recordConsumer.endField(name, count);
            count++;
        }
        recordConsumer.endMessage();
    }

    private void addList(RecordConsumer recordConsumer, ArrowType.PrimitiveType elementType, List<?> list) {
        recordConsumer.startGroup();
        if (!list.isEmpty()) {
            recordConsumer.startField("list", 0);
            recordConsumer.startGroup();
            recordConsumer.startField("element", 0);
            if (elementType.equals(new ArrowType.Int(32, true))) {
                for (Object o : list) {
                    recordConsumer.addInteger((int) o);
                }
            } else if (elementType.equals(new ArrowType.Int(64, true))) {
                for (Object o : list) {
                    recordConsumer.addLong((long) o);
                }
            } else if (elementType instanceof ArrowType.Utf8) {
                for (Object o : list) {
                    recordConsumer.addBinary(Binary.fromString((String) o));
                }
            } else if (elementType instanceof ArrowType.Binary) {
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

    private void addMap(RecordConsumer recordConsumer, ArrowType.PrimitiveType keyType, ArrowType.PrimitiveType valueType, Map<?, ?> map) {
        recordConsumer.startGroup();
        if (!map.isEmpty()) {
            recordConsumer.startField("key_value", 0);
            recordConsumer.startGroup();
            recordConsumer.startField("key", 0);
            if (keyType.equals(new ArrowType.Int(32, true))) {
                for (Object key : map.keySet()) {
                    recordConsumer.addInteger((int) key);
                }
            } else if (keyType.equals(new ArrowType.Int(64, true))) {
                for (Object key : map.keySet()) {
                    recordConsumer.addLong((long) key);
                }
            } else if (keyType instanceof ArrowType.Utf8) {
                for (Object key : map.keySet()) {
                    recordConsumer.addBinary(Binary.fromString((String) key));
                }
            } else if (keyType instanceof ArrowType.Binary) {
                for (Object key : map.keySet()) {
                    recordConsumer.addBinary(Binary.fromConstantByteArray((byte[]) key));
                }
            } else {
                throw new RuntimeException("Unknown type " + keyType);
            }
            recordConsumer.endField("key", 0);
            recordConsumer.startField("value", 1);
            if (valueType.equals(new ArrowType.Int(32, true))) {
                for (Object value : map.values()) {
                    recordConsumer.addInteger((int) value);
                }
            } else if (valueType.equals(new ArrowType.Int(64, true))) {
                for (Object value : map.values()) {
                    recordConsumer.addLong((long) value);
                }
            } else if (valueType instanceof ArrowType.Utf8) {
                for (Object value : map.values()) {
                    recordConsumer.addBinary(Binary.fromString((String) value));
                }
            } else if (valueType instanceof ArrowType.Binary) {
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
