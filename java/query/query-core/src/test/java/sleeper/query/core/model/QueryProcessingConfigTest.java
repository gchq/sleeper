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
package sleeper.query.core.model;

import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryProcessingConfigTest {

    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .sortKeyFields(new Field("sort", new LongType()))
            .valueFields(
                    new Field("value1", new StringType()),
                    new Field("value2", new LongType()),
                    new Field("value3", new StringType()))
            .build();

    @Test
    void shouldKeepAllFieldsWhenNoValueFieldsWereRequested() {
        // Given
        QueryProcessingConfig config = QueryProcessingConfig.none();

        // When / Then
        assertThat(config.narrowToRequestedValueFields(schema)).isEqualTo(schema);
    }

    @Test
    void shouldKeepKeysAndRequestedValueFieldsInSchemaOrder() {
        // Given
        QueryProcessingConfig config = QueryProcessingConfig.none()
                .withRequestedValueFields(List.of("value3", "value1"));

        // When / Then
        assertThat(config.narrowToRequestedValueFields(schema)).isEqualTo(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(
                        new Field("value1", new StringType()),
                        new Field("value3", new StringType()))
                .build());
    }

    @Test
    void shouldKeepOnlyKeysWhenNoValueFieldsAreRequested() {
        // Given
        QueryProcessingConfig config = QueryProcessingConfig.none()
                .withRequestedValueFields(List.of());

        // When / Then
        assertThat(config.narrowToRequestedValueFields(schema)).isEqualTo(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .build());
    }

    @Test
    void shouldIgnoreRequestedFieldThatIsNotInTheTable() {
        // Given
        QueryProcessingConfig config = QueryProcessingConfig.none()
                .withRequestedValueFields(List.of("value2", "not-a-field"));

        // When / Then
        assertThat(config.narrowToRequestedValueFields(schema)).isEqualTo(Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value2", new LongType()))
                .build());
    }
}
