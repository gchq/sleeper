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

package sleeper.arrow.schema;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import sleeper.core.schema.Field;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.Type;

import java.util.List;

public class ConverterTestHelper {
    private ConverterTestHelper() {
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowMapField(ArrowType keyType, ArrowType valueType) {
        return arrowMapField("field", keyType, valueType);
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowMapField(String name, ArrowType keyType, ArrowType valueType) {
        return arrowListField(name,
                arrowStructField(name,
                        arrowField("key", keyType),
                        arrowField("value", valueType)));
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowListField(ArrowType type) {
        return arrowListField("field", type);
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowListField(String name, ArrowType type) {
        return arrowListField(name, arrowField("element", type));
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowListField(String name, org.apache.arrow.vector.types.pojo.Field... fields) {
        return new org.apache.arrow.vector.types.pojo.Field(name, FieldType.notNullable(new ArrowType.List()),
                List.of(fields));
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowStructField(String name, org.apache.arrow.vector.types.pojo.Field... fields) {
        return new org.apache.arrow.vector.types.pojo.Field(
                name + "-key-value-struct",
                new org.apache.arrow.vector.types.pojo.FieldType(false, new ArrowType.Struct(), null),
                List.of(fields));
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowField(ArrowType type) {
        return arrowField("field", type);
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowField(String name, ArrowType type) {
        return org.apache.arrow.vector.types.pojo.Field.notNullable(name, type);
    }

    public static org.apache.arrow.vector.types.pojo.Field arrowFieldNullable(String name, ArrowType type) {
        return org.apache.arrow.vector.types.pojo.Field.nullable(name, type);
    }

    public static Field sleeperMapField(String name, PrimitiveType keyType, PrimitiveType valueType) {
        return sleeperField(name, new MapType(keyType, valueType));
    }

    public static Field sleeperListField(String name, PrimitiveType type) {
        return sleeperField(name, new ListType(type));
    }

    public static Field sleeperField(Type type) {
        return sleeperField("field", type);
    }

    public static Field sleeperField(String name, Type type) {
        return new Field(name, type);
    }
}
