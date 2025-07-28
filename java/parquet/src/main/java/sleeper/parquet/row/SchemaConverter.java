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

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Sleeper schema to Parquet format. Produces a Parquet {@link MessageType}.
 */
class SchemaConverter {

    private SchemaConverter() {
    }

    static MessageType getSchema(Schema schema) {
        List<Field> types = schema.getAllFields();
        List<org.apache.parquet.schema.Type> primitiveTypes = new ArrayList<>();
        for (Field field : types) {
            if (field.getType() instanceof sleeper.core.schema.type.PrimitiveType) {
                primitiveTypes.add(getParquetPrimitiveTypeFromSleeperPrimitiveType(field.getName(),
                        (sleeper.core.schema.type.PrimitiveType) field.getType()));
            } else if (field.getType() instanceof MapType) {
                MapType mapType = (MapType) field.getType();
                sleeper.core.schema.type.PrimitiveType keyType = mapType.getKeyType();
                PrimitiveType keyParquetType = getParquetPrimitiveTypeFromSleeperPrimitiveType("key", keyType);
                sleeper.core.schema.type.PrimitiveType valueType = mapType.getValueType();
                PrimitiveType valueParquetType = getParquetPrimitiveTypeFromSleeperPrimitiveType("value", valueType);
                GroupType mapGroupType = Types.requiredMap()
                        .key(keyParquetType)
                        .value(valueParquetType)
                        .named(field.getName());
                primitiveTypes.add(mapGroupType);
            } else if (field.getType() instanceof ListType) {
                ListType listType = (ListType) field.getType();
                sleeper.core.schema.type.PrimitiveType elementType = listType.getElementType();
                PrimitiveType elementParquetType = getParquetPrimitiveTypeFromSleeperPrimitiveType(field.getName(), elementType);
                GroupType listGroupType = Types.requiredList()
                        .element(elementParquetType)
                        .named(field.getName());
                primitiveTypes.add(listGroupType);
            } else {
                throw new IllegalArgumentException("Field with unknown type (" + field + ")");
            }
        }
        return new MessageType("record", primitiveTypes);
    }

    private static PrimitiveType getParquetPrimitiveTypeFromSleeperPrimitiveType(String name, sleeper.core.schema.type.PrimitiveType primitiveType) {
        if (primitiveType instanceof IntType) {
            return Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                    .named(name);
        }
        if (primitiveType instanceof LongType) {
            return Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .named(name);
        }
        if (primitiveType instanceof StringType) {
            return Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named(name);
        }
        if (primitiveType instanceof ByteArrayType) {
            return Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                    .named(name);
        }
        throw new IllegalArgumentException("Unknown type " + primitiveType);
    }
}
