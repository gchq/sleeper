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

package sleeper.systemtest.suite.fixtures;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

public class SystemTestSchema {
    private SystemTestSchema() {
    }

    public static final String ROW_KEY_FIELD = "key";
    public static final String SORT_KEY_FIELD = "timestamp";
    public static final String VALUE_FIELD = "value";
    public static final Schema DEFAULT_SCHEMA = Schema.builder()
            .rowKeyFields(new Field(ROW_KEY_FIELD, new StringType()))
            .sortKeyFields(new Field(SORT_KEY_FIELD, new LongType()))
            .valueFields(new Field(VALUE_FIELD, new StringType()))
            .build();
}
