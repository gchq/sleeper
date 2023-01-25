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

package sleeper.core.schema.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;

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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ArrowConverter {
    private ArrowConverter() {
    }

    public static final String MAP_KEY_FIELD_NAME = "key";
    public static final String MAP_VALUE_FIELD_NAME = "value";

    /**
     * Create an Arrow Schema from a Sleeper Schema. The order of the fields in each Schema is retained.
     *
     * @param sleeperSchema The Sleeper {@link Schema}
     * @return The Arrow {@link org.apache.arrow.vector.types.pojo.Schema}
     */
    public static org.apache.arrow.vector.types.pojo.Schema convertSleeperSchemaToArrowSchema(Schema sleeperSchema) {
        List<org.apache.arrow.vector.types.pojo.Field> arrowFields =
                sleeperSchema.getAllFields().stream()
                        .map(ArrowConverter::convertSleeperFieldToArrowField)
                        .collect(Collectors.toList());
        return new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
    }

    public static org.apache.arrow.vector.types.pojo.Field convertSleeperFieldToArrowField(Field sleeperField) {
        String fieldName = sleeperField.getName();
        Type sleeperType = sleeperField.getType();
        if (sleeperType instanceof IntType ||
                sleeperType instanceof LongType ||
                sleeperType instanceof StringType ||
                sleeperType instanceof ByteArrayType) {
            // Where the Sleeper field type is a straightforward primitive type, the corresponding Arrow type is used
            return convertSleeperPrimitiveFieldToArrowField(sleeperField);
        } else if (sleeperType instanceof ListType) {
            // Where the Sleeper field type is a list, the Arrow field type is also a list. The elements of the
            // Arrow list are chosen to match the (primitive) type of the elements in the Sleeper list
            Type elementSleeperType = ((ListType) sleeperType).getElementType();
            Field elementSleeperField = new Field("element", elementSleeperType);
            org.apache.arrow.vector.types.pojo.Field elementArrowField = convertSleeperPrimitiveFieldToArrowField(elementSleeperField);
            return new org.apache.arrow.vector.types.pojo.Field(
                    fieldName,
                    new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.List(), null),
                    Collections.singletonList(elementArrowField));
        } else if (sleeperType instanceof MapType) {
            // Where the Sleeper field type is a map, the Arrow field type is a list. Each element of the list is an
            // Arrow struct with two members: key and value. The types of the key and value are chosen to match the
            // (primitive) type of the elements in the Sleeper list.
            // This implementation does not use the Arrow 'map' field type, as we were unable to make this approach work
            // in our experiments.
            Type keySleeperType = ((MapType) sleeperType).getKeyType();
            Type valueSleeperType = ((MapType) sleeperType).getValueType();
            Field keySleeperField = new Field(MAP_KEY_FIELD_NAME, keySleeperType);
            Field valueSleeperField = new Field(MAP_VALUE_FIELD_NAME, valueSleeperType);
            org.apache.arrow.vector.types.pojo.Field keyArrowField = convertSleeperPrimitiveFieldToArrowField(keySleeperField);
            org.apache.arrow.vector.types.pojo.Field valueArrowField = convertSleeperPrimitiveFieldToArrowField(valueSleeperField);
            org.apache.arrow.vector.types.pojo.Field elementArrowStructField = new org.apache.arrow.vector.types.pojo.Field(
                    fieldName + "-key-value-struct",
                    new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.Struct(), null),
                    Stream.of(keyArrowField, valueArrowField).collect(Collectors.toList()));
            return new org.apache.arrow.vector.types.pojo.Field(
                    fieldName,
                    new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.List(), null),
                    Collections.singletonList(elementArrowStructField));
        } else {
            throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not handled");
        }
    }

    /**
     * Convert a primitive Sleeper field into an Arrow field.
     *
     * @param sleeperField The Sleeper field to be converted
     * @return The corresponding Arrow field
     */
    private static org.apache.arrow.vector.types.pojo.Field convertSleeperPrimitiveFieldToArrowField(Field sleeperField) {
        String fieldName = sleeperField.getName();
        Type sleeperType = sleeperField.getType();
        if (sleeperType instanceof IntType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Int(32, true));
        } else if (sleeperType instanceof LongType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Int(64, true));
        } else if (sleeperType instanceof StringType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Utf8());
        } else if (sleeperType instanceof ByteArrayType) {
            return org.apache.arrow.vector.types.pojo.Field.notNullable(fieldName, new ArrowType.Binary());
        } else {
            throw new UnsupportedOperationException("Sleeper column type " + sleeperType.toString() + " is not a primitive inside convertSleeperPrimitiveFieldToArrowField()");
        }
    }

    public static Field convertArrowFieldToSleeperField(org.apache.arrow.vector.types.pojo.Field arrowField) {
        ArrowType type = arrowField.getType();
        if (type instanceof ArrowType.PrimitiveType) {
            return convertArrowPrimitiveFieldToSleeperField(arrowField);
        } else if (type instanceof ArrowType.List) {
            return convertArrowNonPrimitiveFieldToSleeperField(arrowField);
        } else {
            throw new UnsupportedOperationException("Arrow column type " + type.toString() + " is not supported by Sleeper");
        }
    }

    private static Field convertArrowPrimitiveFieldToSleeperField(org.apache.arrow.vector.types.pojo.Field arrowField) {
        String fieldName = arrowField.getName();
        ArrowType type = arrowField.getType();
        return new Field(fieldName, convertArrowPrimitiveTypeToSleeperType(type));
    }

    private static PrimitiveType convertArrowPrimitiveTypeToSleeperType(ArrowType type) {
        if (type instanceof ArrowType.Int) {
            ArrowType.Int arrowIntType = (ArrowType.Int) type;
            if (arrowIntType.getBitWidth() == 64 && arrowIntType.getIsSigned()) {
                return new LongType();
            } else if (arrowIntType.getBitWidth() == 32 && arrowIntType.getIsSigned()) {
                return new IntType();
            } else {
                throw new UnsupportedOperationException("Arrow int type with bitWidth=" + arrowIntType.getBitWidth() +
                        " and signed=" + arrowIntType.getIsSigned() + " is not supported by Sleeper");
            }
        } else if (type instanceof ArrowType.Binary) {
            return new ByteArrayType();
        } else if (type instanceof ArrowType.Utf8) {
            return new StringType();
        } else {
            throw new UnsupportedOperationException("Arrow column type " + type.toString() + " is not supported by Sleeper");
        }
    }

    private static Field convertArrowNonPrimitiveFieldToSleeperField(org.apache.arrow.vector.types.pojo.Field arrowField) {
        if (arrowField.getChildren().size() != 1) {
            throw new UnsupportedOperationException("Arrow list field does not contain exactly one field element, which is not supported by Sleeper");
        }
        ArrowType listFieldType = arrowField.getChildren().get(0).getType();
        if (listFieldType instanceof ArrowType.PrimitiveType) {
            return new Field(arrowField.getName(), new ListType(
                    convertArrowPrimitiveTypeToSleeperType(listFieldType)));
        } else if (listFieldType instanceof ArrowType.Struct) {
            return convertArrowStructListFieldToSleeperField(arrowField);
        } else {
            throw new UnsupportedOperationException("Arrow list field contains unsupported field types");
        }
    }

    private static Field convertArrowStructListFieldToSleeperField(org.apache.arrow.vector.types.pojo.Field arrowField) {
        org.apache.arrow.vector.types.pojo.Field structField = arrowField.getChildren().get(0);
        if (structField.getChildren().size() != 2) {
            throw new UnsupportedOperationException("Arrow struct field does not contain exactly two field elements, which is not supported by Sleeper");
        }
        if (structField.getChildren().stream().anyMatch(c -> !(c.getType() instanceof ArrowType.PrimitiveType))) {
            throw new UnsupportedOperationException("Arrow struct field contains non-primitive field types, which is not supported by Sleeper");
        }
        List<org.apache.arrow.vector.types.pojo.Field> structChildren = structField.getChildren();
        return new Field(arrowField.getName(), new MapType(
                convertArrowPrimitiveTypeToSleeperType(structChildren.get(0).getType()),
                convertArrowPrimitiveTypeToSleeperType(structChildren.get(1).getType())));
    }
}
