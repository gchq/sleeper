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
package sleeper.core.schema;

import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;

/**
 * A test helper for creating schema objects.
 */
public class SchemaTestHelper {

    private SchemaTestHelper() {
    }

    /**
     * Creates a Schema with a single row key field. This field is a {@link LongType}.
     *
     * @param  key the name of the row key field
     * @return     a {@link Schema} with one row key field
     */
    public static Schema createSchemaWithKey(String key) {
        return createSchemaWithKey(key, new LongType());
    }

    /**
     * Creates a Schema with a single row key field.
     *
     * @param  key  the name of the row key field
     * @param  type the type of the row key field
     * @return      a {@link Schema} with one row key field
     */
    public static Schema createSchemaWithKey(String key, PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field(key, type)).build();
    }

    /**
     * Creates a Schema with a two row key fields.
     *
     * @param  key1  the name of the first row key field
     * @param  type1 the type of the first row key field
     * @param  key2  the name of the second row key field
     * @param  type2 the type of the second row key field
     * @return       a {@link Schema} with two row key fields
     */
    public static Schema createSchemaWithMultipleKeys(String key1, PrimitiveType type1, String key2, PrimitiveType type2) {
        return Schema.builder().rowKeyFields(new Field(key1, type1), new Field(key2, type2)).build();
    }
}
