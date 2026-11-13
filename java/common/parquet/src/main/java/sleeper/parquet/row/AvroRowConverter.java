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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import sleeper.core.row.Row;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts Avro generic records read from a Parquet file into Sleeper row objects.
 */
class AvroRowConverter {

    private AvroRowConverter() {
    }

    /**
     * Converts a record to a Sleeper row.
     *
     * @param  record     the record
     * @param  fileSchema the Parquet schema of the file the record was read from, used to find which fields are maps
     *                    that were read as lists, see {@link GenericRowReadSupport}
     * @return            the row
     */
    static Row toRow(GenericRecord record, GroupType fileSchema) {
        Row row = new Row();
        for (Schema.Field field : record.getSchema().getFields()) {
            row.put(field.name(), toJavaValue(record.get(field.pos()), fieldTypeOrNull(fileSchema, field.name())));
        }
        return row;
    }

    private static Object toJavaValue(Object value, Type parquetType) {
        if (value instanceof CharSequence) {
            return value.toString();
        } else if (value instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        } else if (value instanceof GenericFixed fixed) {
            return fixed.bytes().clone();
        } else if (value instanceof List<?> list) {
            if (parquetType != null && GenericRowReadSupport.isMapWithNonStringKey(parquetType)) {
                return keyValueRecordsToMap(list, parquetType.asGroupType().getType(0).asGroupType());
            }
            List<Object> converted = new ArrayList<>(list.size());
            list.forEach(element -> converted.add(toJavaValue(element, null)));
            return converted;
        } else if (value instanceof Map<?, ?> map) {
            Type valueType = parquetType != null && GenericRowReadSupport.isMap(parquetType)
                    ? parquetType.asGroupType().getType(0).asGroupType().getType(1)
                    : null;
            Map<Object, Object> converted = new HashMap<>();
            map.forEach((key, mapValue) -> converted.put(toJavaValue(key, null), toJavaValue(mapValue, valueType)));
            return converted;
        } else if (value instanceof GenericRecord record) {
            GroupType groupType = parquetType != null && !parquetType.isPrimitive() ? parquetType.asGroupType() : null;
            Map<String, Object> converted = new LinkedHashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                converted.put(field.name(), toJavaValue(record.get(field.pos()), fieldTypeOrNull(groupType, field.name())));
            }
            return converted;
        } else {
            return value;
        }
    }

    private static Map<Object, Object> keyValueRecordsToMap(List<?> keyValues, GroupType keyValueType) {
        Map<Object, Object> map = new HashMap<>();
        for (Object keyValue : keyValues) {
            GenericRecord record = (GenericRecord) keyValue;
            map.put(toJavaValue(record.get(0), keyValueType.getType(0)), toJavaValue(record.get(1), keyValueType.getType(1)));
        }
        return map;
    }

    private static Type fieldTypeOrNull(GroupType groupType, String fieldName) {
        if (groupType != null && groupType.containsField(fieldName)) {
            return groupType.getType(fieldName);
        } else {
            return null;
        }
    }
}
