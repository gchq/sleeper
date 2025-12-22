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
package sleeper.bulkimport.runner.common;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

/**
 * Converts a Sleeper schema into a Spark struct type.
 */
public class StructTypeFactory {

    public StructType getStructType(Schema schema) {
        int numberFields = schema.getAllFields().size();
        StructField[] structFields = new StructField[numberFields];
        int i = 0;
        for (Field field : schema.getAllFields()) {
            structFields[i++] = getStructField(field);
        }
        return new StructType(structFields);
    }

    private StructField getStructField(Field field) {
        return StructField.apply(field.getName(), getDataType(field.getType()), false, Metadata.empty());
    }

    private DataType getDataType(Type type) {
        if (type instanceof IntType) {
            return DataTypes.IntegerType;
        } else if (type instanceof LongType) {
            return DataTypes.LongType;
        } else if (type instanceof StringType) {
            return DataTypes.StringType;
        } else if (type instanceof ByteArrayType) {
            return DataTypes.BinaryType;
        } else if (type instanceof ListType) {
            PrimitiveType primitiveType = ((ListType) type).getElementType();
            return DataTypes.createArrayType(getDataType(primitiveType));
        } else if (type instanceof MapType) {
            PrimitiveType keyType = ((MapType) type).getKeyType();
            PrimitiveType valueType = ((MapType) type).getValueType();
            return DataTypes.createMapType(getDataType(keyType), getDataType(valueType));
        } else {
            throw new IllegalArgumentException("Unknown type " + type);
        }
    }
}
