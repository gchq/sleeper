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
package sleeper.core.schema;

import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;

public class SchemaTestHelper {

    private SchemaTestHelper() {
    }

    public static Schema schemaWithKey(String key) {
        return schemaWithKey(key, new LongType());
    }

    public static Schema schemaWithKey(String key, PrimitiveType type) {
        return Schema.builder().rowKeyFields(new Field(key, type)).build();
    }

}
