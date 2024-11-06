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
package sleeper.parquet.record;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts rows of Parquet data into Sleeper records.
 */
public class RecordConverter extends GroupConverter {
    private final Record currentRecord;
    private final Converter[] converters;

    public RecordConverter(Schema schema) {
        currentRecord = new Record();
        List<Field> fields = schema.getAllFields();
        this.converters = new Converter[fields.size()];
        int count = 0;
        for (Field field : fields) {
            if (field.getType() instanceof IntType) {
                this.converters[count] = new IntConverter(field.getName(), currentRecord);
            } else if (field.getType() instanceof LongType) {
                this.converters[count] = new LongConverter(field.getName(), currentRecord);
            } else if (field.getType() instanceof StringType) {
                this.converters[count] = new StringConverter(field.getName(), currentRecord);
            } else if (field.getType() instanceof ByteArrayType) {
                this.converters[count] = new ByteArrayConverter(field.getName(), currentRecord);
            } else if (field.getType() instanceof MapType) {
                MapType mapType = (MapType) field.getType();
                PrimitiveType keyType = mapType.getKeyType();
                PrimitiveType valueType = mapType.getValueType();
                this.converters[count] = new MapConverter<>(field.getName(), keyType, valueType, currentRecord);
            } else if (field.getType() instanceof ListType) {
                ListType listType = (ListType) field.getType();
                PrimitiveType elementType = listType.getElementType();
                this.converters[count] = new ListConverter<>(field.getName(), elementType, currentRecord);
            } else {
                throw new IllegalArgumentException("Schema has a field with an unknown type (" + field + ")");
            }
            count++;
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
    }

    public Record getRecord() {
        return currentRecord;
    }

    public static class IntConverter extends PrimitiveConverter {
        private final String name;
        private final Record record;

        public IntConverter(String name, Record record) {
            this.name = name;
            this.record = record;
        }

        @Override
        public void addInt(int value) {
            record.put(name, value);
        }
    }

    public static class LongConverter extends PrimitiveConverter {
        private final String name;
        private final Record record;

        public LongConverter(String name, Record record) {
            this.name = name;
            this.record = record;
        }

        @Override
        public void addLong(long value) {
            record.put(name, value);
        }
    }

    public static class StringConverter extends PrimitiveConverter {
        private final String name;
        private final Record record;

        public StringConverter(String name, Record record) {
            this.name = name;
            this.record = record;
        }

        @Override
        public void addBinary(Binary value) {
            record.put(name, value.toStringUsingUTF8());
        }
    }

    public static class ByteArrayConverter extends PrimitiveConverter {
        private final String name;
        private final Record record;

        public ByteArrayConverter(String name, Record record) {
            this.name = name;
            this.record = record;
        }

        @Override
        public void addBinary(Binary value) {
            record.put(name, value.getBytes());
        }
    }

    public static class ListConverter<E> extends GroupConverter {
        private final String name;
        private final Record record;
        private final List<E> elements;
        private final ElementConverter<E> elementConverter;

        public ListConverter(String name, PrimitiveType elementType, Record record) {
            this.name = name;
            this.record = record;
            this.elements = new ArrayList<>();
            this.elementConverter = new ElementConverter<>(elements, elementType);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (0 != fieldIndex) {
                throw new RuntimeException("Only 0 is a valid field index");
            }
            return elementConverter;
        }

        @Override
        public void start() {
            elements.clear();
        }

        @Override
        public void end() {
            List<E> list = new ArrayList<>(elements);
            record.put(name, list);
        }
    }

    public static class MapConverter<K, V> extends GroupConverter {
        private final String name;
        private final Record record;
        private final List<K> keys;
        private final List<V> values;
        private final KeyValueConverter<K, V> keyValueConverter;

        public MapConverter(String name, PrimitiveType keyType, PrimitiveType valueType, Record record) {
            this.name = name;
            this.record = record;
            this.keys = new ArrayList<>();
            this.values = new ArrayList<>();
            this.keyValueConverter = new KeyValueConverter<>(keys, values, keyType, valueType);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (0 != fieldIndex) {
                throw new RuntimeException("Only 0 is a valid field index");
            }
            return keyValueConverter;
        }

        @Override
        public void start() {
            keys.clear();
            values.clear();
        }

        @Override
        public void end() {
            Map<K, V> map = new HashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                map.put(keys.get(i), values.get(i));
            }
            record.put(name, map);
        }
    }

    private static class IntInMapConverter extends PrimitiveConverter {
        private final List<Integer> list;

        private IntInMapConverter(List<Integer> list) {
            this.list = list;
        }

        @Override
        public void addInt(int value) {
            list.add(value);
        }
    }

    private static class LongInMapConverter extends PrimitiveConverter {
        private final List<Long> list;

        private LongInMapConverter(List<Long> list) {
            this.list = list;
        }

        @Override
        public void addLong(long value) {
            list.add(value);
        }
    }

    private static class StringInMapConverter extends PrimitiveConverter {
        private final List<String> list;

        private StringInMapConverter(List<String> list) {
            this.list = list;
        }

        @Override
        public void addBinary(Binary value) {
            list.add(value.toStringUsingUTF8());
        }
    }

    private static class ByteArrayInMapConverter extends PrimitiveConverter {
        private final List<byte[]> list;

        private ByteArrayInMapConverter(List<byte[]> list) {
            this.list = list;
        }

        @Override
        public void addBinary(Binary value) {
            list.add(value.getBytes());
        }
    }

    private static class KeyValueConverter<K, V> extends GroupConverter {
        private final PrimitiveConverter keyConverter;
        private final PrimitiveConverter valueConverter;

        private KeyValueConverter(List<K> keys, List<V> values,
                PrimitiveType keyType, PrimitiveType valueType) {
            this.keyConverter = getInListConverter(keyType, keys);
            this.valueConverter = getInListConverter(valueType, values);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (0 == fieldIndex) {
                return keyConverter;
            }
            if (1 == fieldIndex) {
                return valueConverter;
            }
            throw new IllegalArgumentException("Invalid fieldIndex - should be 0 or 1");
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
        }
    }

    private static class ElementConverter<E> extends GroupConverter {
        private final PrimitiveConverter elementConverter;

        private ElementConverter(List<E> elements, PrimitiveType elementType) {
            this.elementConverter = getInListConverter(elementType, elements);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            if (0 == fieldIndex) {
                return elementConverter;
            }
            throw new IllegalArgumentException("Invalid fieldIndex - should be 0");
        }

        @Override
        public void start() {
        }

        @Override
        public void end() {
        }
    }

    private static <T> PrimitiveConverter getInListConverter(PrimitiveType type, List<T> list) {
        if (type instanceof IntType) {
            return new IntInMapConverter((List<Integer>) list);
        }
        if (type instanceof LongType) {
            return new LongInMapConverter((List<Long>) list);
        }
        if (type instanceof StringType) {
            return new StringInMapConverter((List<String>) list);
        }
        if (type instanceof ByteArrayType) {
            return new ByteArrayInMapConverter((List<byte[]>) list);
        }
        throw new IllegalArgumentException("Unknown type " + type);
    }
}
