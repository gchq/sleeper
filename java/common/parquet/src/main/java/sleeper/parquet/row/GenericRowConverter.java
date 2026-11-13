/*
 * Copyright 2022-2026 Crown Copyright
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
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import sleeper.core.row.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

/**
 * Converts rows of Parquet data into Sleeper rows, based on the Parquet schema of the data rather than a Sleeper
 * schema. A new row object is created for each row read.
 * <p>
 * Values are read as the following Java types:
 * <ul>
 * <li>Strings, enums and JSON are read as a string</li>
 * <li>Decimals are read as a {@link BigDecimal}</li>
 * <li>Dates are read as a {@link LocalDate}</li>
 * <li>Timestamps are read as an {@link Instant} if they are adjusted to UTC, or otherwise as a
 * {@link LocalDateTime}. Legacy int96 timestamps are read as an {@link Instant}</li>
 * <li>Times of day are read as a {@link LocalTime}</li>
 * <li>UUIDs are read as a {@link UUID}</li>
 * <li>Unsigned 32 bit integers are read as a Long, and unsigned 64 bit integers as a {@link BigInteger}</li>
 * <li>Other binary and fixed length binary values are read as a byte array</li>
 * <li>Other primitive values are read as an Integer, Long, Float, Double or Boolean, as they are held in
 * Parquet</li>
 * <li>Lists and repeated fields are read as a list</li>
 * <li>Maps are read as a map, with any type of key</li>
 * <li>Any other group is read as a map from field name to value</li>
 * <li>An optional field with no value is read as null</li>
 * </ul>
 */
class GenericRowConverter extends GroupConverter {
    private static final long JULIAN_DAY_OF_EPOCH = 2_440_588L;
    private static final long SECONDS_PER_DAY = 86_400L;
    private final Fields fields;
    private Row row = new Row();

    GenericRowConverter(GroupType schema) {
        this.fields = new Fields(schema, (name, value) -> row.put(name, value));
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return fields.converters[fieldIndex];
    }

    @Override
    public void start() {
        row = new Row();
        fields.start();
    }

    @Override
    public void end() {
    }

    public Row getRow() {
        return row;
    }

    private interface FieldSetter {
        void set(String name, Object value);
    }

    private static Converter converterFor(Type type, Consumer<Object> setter) {
        if (type.isPrimitive()) {
            return primitiveConverterFor(type.asPrimitiveType(), setter);
        }
        GroupType group = type.asGroupType();
        LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
        if (annotation instanceof ListLogicalTypeAnnotation && isRepeatedOnly(group)) {
            return new ListConverter(group, setter);
        }
        if ((annotation instanceof MapLogicalTypeAnnotation || annotation instanceof MapKeyValueTypeAnnotation)
                && isRepeatedOnly(group) && !group.getType(0).isPrimitive() && group.getType(0).asGroupType().getFieldCount() == 2) {
            return new MapConverter(group, setter);
        }
        return new StructConverter(group, setter);
    }

    private static Converter primitiveConverterFor(PrimitiveType type, Consumer<Object> setter) {
        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        PrimitiveTypeName primitive = type.getPrimitiveTypeName();
        boolean isInt = primitive == PrimitiveTypeName.INT32;
        boolean isLong = primitive == PrimitiveTypeName.INT64;
        boolean isBinary = primitive == PrimitiveTypeName.BINARY || primitive == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        if (isString(type) && isBinary) {
            return new BinaryConverter(setter, Binary::toStringUsingUTF8);
        } else if (annotation instanceof DecimalLogicalTypeAnnotation decimal) {
            int scale = decimal.getScale();
            if (isInt) {
                return new IntConverter(setter, value -> BigDecimal.valueOf(value, scale));
            } else if (isLong) {
                return new LongConverter(setter, value -> BigDecimal.valueOf(value, scale));
            } else if (isBinary) {
                return new BinaryConverter(setter, value -> new BigDecimal(new BigInteger(value.getBytes()), scale));
            }
        } else if (annotation instanceof DateLogicalTypeAnnotation && isInt) {
            return new IntConverter(setter, LocalDate::ofEpochDay);
        } else if (annotation instanceof TimestampLogicalTypeAnnotation timestamp && isLong) {
            TimeUnit unit = timestamp.getUnit();
            if (timestamp.isAdjustedToUTC()) {
                return new LongConverter(setter, value -> toInstant(value, unit));
            } else {
                return new LongConverter(setter, value -> LocalDateTime.ofInstant(toInstant(value, unit), ZoneOffset.UTC));
            }
        } else if (annotation instanceof TimeLogicalTypeAnnotation time) {
            long nanosPerUnit = nanosPerUnit(time.getUnit());
            if (isInt) {
                return new IntConverter(setter, value -> LocalTime.ofNanoOfDay(value * nanosPerUnit));
            } else if (isLong) {
                return new LongConverter(setter, value -> LocalTime.ofNanoOfDay(value * nanosPerUnit));
            }
        } else if (annotation instanceof UUIDLogicalTypeAnnotation && isBinary) {
            return new BinaryConverter(setter, value -> {
                ByteBuffer buffer = ByteBuffer.wrap(value.getBytes());
                return new UUID(buffer.getLong(), buffer.getLong());
            });
        } else if (annotation instanceof IntLogicalTypeAnnotation intType && !intType.isSigned()) {
            if (isInt && intType.getBitWidth() == 32) {
                return new IntConverter(setter, Integer::toUnsignedLong);
            } else if (isLong) {
                return new LongConverter(setter, value -> new BigInteger(Long.toUnsignedString(value)));
            }
        } else if (primitive == PrimitiveTypeName.INT96) {
            return new BinaryConverter(setter, GenericRowConverter::int96ToInstant);
        }
        return new ValueConverter(setter);
    }

    private static Instant toInstant(long value, TimeUnit unit) {
        long unitsPerSecond = 1_000_000_000L / nanosPerUnit(unit);
        return Instant.ofEpochSecond(
                Math.floorDiv(value, unitsPerSecond),
                Math.floorMod(value, unitsPerSecond) * nanosPerUnit(unit));
    }

    private static long nanosPerUnit(TimeUnit unit) {
        switch (unit) {
            case MILLIS:
                return 1_000_000L;
            case MICROS:
                return 1_000L;
            case NANOS:
                return 1L;
            default:
                throw new IllegalArgumentException("Unrecognised time unit: " + unit);
        }
    }

    // An int96 timestamp holds the nanoseconds within the day, followed by the Julian day number
    private static Instant int96ToInstant(Binary value) {
        ByteBuffer buffer = ByteBuffer.wrap(value.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buffer.getLong();
        long julianDay = buffer.getInt();
        return Instant.ofEpochSecond((julianDay - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY, nanosOfDay);
    }

    private static boolean isRepeatedOnly(GroupType group) {
        return group.getFieldCount() == 1 && group.getType(0).isRepetition(Type.Repetition.REPEATED);
    }

    private static boolean isString(Type type) {
        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        return annotation instanceof StringLogicalTypeAnnotation
                || annotation instanceof EnumLogicalTypeAnnotation
                || annotation instanceof JsonLogicalTypeAnnotation;
    }

    /**
     * Converts the fields of a group, which may be the whole row or a group nested in it
     */
    private static class Fields {
        private final FieldSetter setter;
        private final Converter[] converters;
        private final List<String> optionalFieldNames = new ArrayList<>();
        private final List<RepeatedField> repeatedFields = new ArrayList<>();

        Fields(GroupType group, FieldSetter setter) {
            this.setter = setter;
            this.converters = new Converter[group.getFieldCount()];
            for (int i = 0; i < converters.length; i++) {
                Type type = group.getType(i);
                String name = type.getName();
                if (type.isRepetition(Type.Repetition.REPEATED)) {
                    RepeatedField repeated = new RepeatedField(name);
                    repeatedFields.add(repeated);
                    converters[i] = converterFor(type, value -> repeated.values.add(value));
                } else {
                    if (type.isRepetition(Type.Repetition.OPTIONAL)) {
                        optionalFieldNames.add(name);
                    }
                    converters[i] = converterFor(type, value -> setter.set(name, value));
                }
            }
        }

        void start() {
            for (String name : optionalFieldNames) {
                setter.set(name, null);
            }
            for (RepeatedField repeated : repeatedFields) {
                repeated.values = new ArrayList<>();
                setter.set(repeated.name, repeated.values);
            }
        }
    }

    // A repeated field that is not held in a list group, read as a list
    private static class RepeatedField {
        private final String name;
        private List<Object> values = new ArrayList<>();

        RepeatedField(String name) {
            this.name = name;
        }
    }

    // Reads values that are held in the row as they are held in Parquet
    private static class ValueConverter extends PrimitiveConverter {
        private final Consumer<Object> setter;

        ValueConverter(Consumer<Object> setter) {
            this.setter = setter;
        }

        @Override
        public void addBinary(Binary value) {
            setter.accept(value.getBytes());
        }

        @Override
        public void addBoolean(boolean value) {
            setter.accept(value);
        }

        @Override
        public void addDouble(double value) {
            setter.accept(value);
        }

        @Override
        public void addFloat(float value) {
            setter.accept(value);
        }

        @Override
        public void addInt(int value) {
            setter.accept(value);
        }

        @Override
        public void addLong(long value) {
            setter.accept(value);
        }
    }

    private static class IntConverter extends PrimitiveConverter {
        private final Consumer<Object> setter;
        private final IntFunction<Object> convert;

        IntConverter(Consumer<Object> setter, IntFunction<Object> convert) {
            this.setter = setter;
            this.convert = convert;
        }

        @Override
        public void addInt(int value) {
            setter.accept(convert.apply(value));
        }
    }

    private static class LongConverter extends PrimitiveConverter {
        private final Consumer<Object> setter;
        private final LongFunction<Object> convert;

        LongConverter(Consumer<Object> setter, LongFunction<Object> convert) {
            this.setter = setter;
            this.convert = convert;
        }

        @Override
        public void addLong(long value) {
            setter.accept(convert.apply(value));
        }
    }

    private static class BinaryConverter extends PrimitiveConverter {
        private final Consumer<Object> setter;
        private final Function<Binary, Object> convert;

        BinaryConverter(Consumer<Object> setter, Function<Binary, Object> convert) {
            this.setter = setter;
            this.convert = convert;
        }

        @Override
        public void addBinary(Binary value) {
            setter.accept(convert.apply(value));
        }
    }

    private static class StructConverter extends GroupConverter {
        private final Consumer<Object> setter;
        private final Fields fields;
        private Map<String, Object> struct = new LinkedHashMap<>();

        StructConverter(GroupType group, Consumer<Object> setter) {
            this.setter = setter;
            this.fields = new Fields(group, (name, value) -> struct.put(name, value));
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return fields.converters[fieldIndex];
        }

        @Override
        public void start() {
            struct = new LinkedHashMap<>();
            fields.start();
        }

        @Override
        public void end() {
            setter.accept(struct);
        }
    }

    private static class ListConverter extends GroupConverter {
        private final Consumer<Object> setter;
        private final Converter repeatedConverter;
        private List<Object> list = new ArrayList<>();

        ListConverter(GroupType listType, Consumer<Object> setter) {
            this.setter = setter;
            Type repeated = listType.getType(0);
            if (isElement(repeated, listType.getName())) {
                repeatedConverter = converterFor(repeated, element -> list.add(element));
            } else {
                repeatedConverter = new WrappedElementConverter(repeated.asGroupType().getType(0), element -> list.add(element));
            }
        }

        // Follows the backward compatibility rules for lists in the Parquet format specification. Normally a list
        // holds a repeated group that wraps each element, but older writers put the elements directly in the list.
        private static boolean isElement(Type repeated, String listName) {
            return repeated.isPrimitive()
                    || repeated.asGroupType().getFieldCount() != 1
                    || repeated.getName().equals("array")
                    || repeated.getName().equals(listName + "_tuple");
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return repeatedConverter;
        }

        @Override
        public void start() {
            list = new ArrayList<>();
        }

        @Override
        public void end() {
            setter.accept(list);
        }
    }

    // Converts the repeated group in a list that wraps each element. The element is null if it has no value.
    private static class WrappedElementConverter extends GroupConverter {
        private final Consumer<Object> setter;
        private final Converter elementConverter;
        private Object element;

        WrappedElementConverter(Type elementType, Consumer<Object> setter) {
            this.setter = setter;
            this.elementConverter = converterFor(elementType, value -> element = value);
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return elementConverter;
        }

        @Override
        public void start() {
            element = null;
        }

        @Override
        public void end() {
            setter.accept(element);
        }
    }

    private static class MapConverter extends GroupConverter {
        private final Consumer<Object> setter;
        private final KeyValueConverter keyValueConverter;
        private Map<Object, Object> map = new HashMap<>();

        MapConverter(GroupType mapType, Consumer<Object> setter) {
            this.setter = setter;
            this.keyValueConverter = new KeyValueConverter(mapType.getType(0).asGroupType());
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            return keyValueConverter;
        }

        @Override
        public void start() {
            map = new HashMap<>();
        }

        @Override
        public void end() {
            setter.accept(map);
        }

        private class KeyValueConverter extends GroupConverter {
            private final Converter keyConverter;
            private final Converter valueConverter;
            private Object key;
            private Object value;

            KeyValueConverter(GroupType keyValueType) {
                this.keyConverter = converterFor(keyValueType.getType(0), read -> key = read);
                this.valueConverter = converterFor(keyValueType.getType(1), read -> value = read);
            }

            @Override
            public Converter getConverter(int fieldIndex) {
                return fieldIndex == 0 ? keyConverter : valueConverter;
            }

            @Override
            public void start() {
                key = null;
                value = null;
            }

            @Override
            public void end() {
                map.put(key, value);
            }
        }
    }
}
