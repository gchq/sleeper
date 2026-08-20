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

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
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
import java.util.Objects;

/**
 * Converts a Sleeper schema to Parquet format. Produces a Parquet {@link MessageType}.
 */
public class SchemaConverter {

    private SchemaConverter() {
    }

    /**
     * Checks whether a Parquet file's schema is compatible with a Sleeper table schema for ingest.
     *
     * A file is compatible if, for every field in the table schema, the file has a column of the
     * same name and matching type. Column order and any extra columns in the file are irrelevant,
     * and nullability (required vs optional) is ignored.
     *
     * @param  fileSchema  the schema read from a Parquet file
     * @param  tableSchema the Sleeper table schema
     * @return             true if the file can be ingested into a table with this schema
     */
    public static boolean isFileSchemaCompatibleWithTable(MessageType fileSchema, Schema tableSchema) {
        for (Type expectedField : getSchema(tableSchema).getFields()) {
            if (!fileSchema.containsField(expectedField.getName())) {
                return false;
            }
            if (!typesMatchIgnoringNullability(fileSchema.getType(expectedField.getName()), expectedField)) {
                return false;
            }
        }
        return true;
    }

    // Compares two Parquet types by name and structure, ignoring repetition (required vs optional) at
    // every level, so nullability differences don't make a file incompatible. Recurses into groups for
    // map/list value fields.
    private static boolean typesMatchIgnoringNullability(Type actual, Type expected) {
        if (!actual.getName().equals(expected.getName())) {
            return false;
        }
        if (!Objects.equals(actual.getLogicalTypeAnnotation(), expected.getLogicalTypeAnnotation())) {
            return false;
        }
        if (actual.isPrimitive() != expected.isPrimitive()) {
            return false;
        }
        if (actual.isPrimitive()) {
            PrimitiveType actualPrimitive = actual.asPrimitiveType();
            PrimitiveType expectedPrimitive = expected.asPrimitiveType();
            return actualPrimitive.getPrimitiveTypeName() == expectedPrimitive.getPrimitiveTypeName()
                    && actualPrimitive.getTypeLength() == expectedPrimitive.getTypeLength();
        }
        GroupType actualGroup = actual.asGroupType();
        GroupType expectedGroup = expected.asGroupType();
        if (actualGroup.getFieldCount() != expectedGroup.getFieldCount()) {
            return false;
        }
        for (int i = 0; i < expectedGroup.getFieldCount(); i++) {
            if (!typesMatchIgnoringNullability(actualGroup.getType(i), expectedGroup.getType(i))) {
                return false;
            }
        }
        return true;
    }

    public static MessageType getSchema(Schema schema) {
        List<Field> types = schema.getAllFields();
        List<org.apache.parquet.schema.Type> primitiveTypes = new ArrayList<>();
        for (Field field : types) {
            if (field.getType() instanceof sleeper.core.schema.type.PrimitiveType) {
                primitiveTypes.add(getParquetPrimitiveTypeFromSleeperPrimitiveType(field.getName(),
                        (sleeper.core.schema.type.PrimitiveType) field.getType(), field.isNullable()));
            } else if (field.getType() instanceof MapType) {
                MapType mapType = (MapType) field.getType();
                sleeper.core.schema.type.PrimitiveType keyType = mapType.getKeyType();
                PrimitiveType keyParquetType = getParquetPrimitiveTypeFromSleeperPrimitiveType("key", keyType, false);
                sleeper.core.schema.type.PrimitiveType valueType = mapType.getValueType();
                PrimitiveType valueParquetType = getParquetPrimitiveTypeFromSleeperPrimitiveType("value", valueType, false);
                GroupType mapGroupType = field.isNullable()
                        ? Types.optionalMap().key(keyParquetType).value(valueParquetType).named(field.getName())
                        : Types.requiredMap().key(keyParquetType).value(valueParquetType).named(field.getName());
                primitiveTypes.add(mapGroupType);
            } else if (field.getType() instanceof ListType) {
                ListType listType = (ListType) field.getType();
                sleeper.core.schema.type.PrimitiveType elementType = listType.getElementType();
                PrimitiveType elementParquetType = getParquetPrimitiveTypeFromSleeperPrimitiveType(field.getName(), elementType, false);
                GroupType listGroupType = field.isNullable()
                        ? Types.optionalList().element(elementParquetType).named(field.getName())
                        : Types.requiredList().element(elementParquetType).named(field.getName());
                primitiveTypes.add(listGroupType);
            } else {
                throw new IllegalArgumentException("Field with unknown type (" + field + ")");
            }
        }
        return new MessageType("record", primitiveTypes);
    }

    private static PrimitiveType getParquetPrimitiveTypeFromSleeperPrimitiveType(
            String name, sleeper.core.schema.type.PrimitiveType primitiveType, boolean nullable) {
        if (primitiveType instanceof IntType) {
            return (nullable ? Types.optional(PrimitiveType.PrimitiveTypeName.INT32) : Types.required(PrimitiveType.PrimitiveTypeName.INT32))
                    .named(name);
        }
        if (primitiveType instanceof LongType) {
            return (nullable ? Types.optional(PrimitiveType.PrimitiveTypeName.INT64) : Types.required(PrimitiveType.PrimitiveTypeName.INT64))
                    .named(name);
        }
        if (primitiveType instanceof StringType) {
            return (nullable ? Types.optional(PrimitiveType.PrimitiveTypeName.BINARY) : Types.required(PrimitiveType.PrimitiveTypeName.BINARY))
                    .as(LogicalTypeAnnotation.stringType())
                    .named(name);
        }
        if (primitiveType instanceof ByteArrayType) {
            return (nullable ? Types.optional(PrimitiveType.PrimitiveTypeName.BINARY) : Types.required(PrimitiveType.PrimitiveTypeName.BINARY))
                    .named(name);
        }
        throw new IllegalArgumentException("Unknown type " + primitiveType);
    }
}
