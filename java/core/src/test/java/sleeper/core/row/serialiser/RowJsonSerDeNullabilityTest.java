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
package sleeper.core.row.serialiser;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;
import sleeper.core.schema.type.Type;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class RowJsonSerDeNullabilityTest {

    @ParameterizedTest
    @MethodSource("valueTypes")
    void shouldOmitMissingNonNullableValue(Type type) {
        // Given
        RowJsonSerDe serDe = serDe(type, false);

        // When / Then
        assertThat(serDe.fromJson("{\"key\":1}"))
                .isEqualTo(new Row(Map.of("key", 1)));
    }

    @ParameterizedTest
    @MethodSource("valueTypes")
    void shouldOmitExplicitNullForNonNullableValue(Type type) {
        // Given
        RowJsonSerDe serDe = serDe(type, false);

        // When / Then
        assertThat(serDe.fromJson("{\"key\":1,\"value\":null}"))
                .isEqualTo(new Row(Map.of("key", 1)));
    }

    @ParameterizedTest
    @MethodSource("valueTypes")
    void shouldStoreNullForMissingNullableValue(Type type) {
        // Given
        RowJsonSerDe serDe = serDe(type, true);
        Row expected = new Row(Map.of("key", 1));
        expected.put("value", null);

        // When / Then
        assertThat(serDe.fromJson("{\"key\":1}"))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("valueTypes")
    void shouldStoreExplicitNullForNullableValue(Type type) {
        // Given
        RowJsonSerDe serDe = serDe(type, true);
        Row expected = new Row(Map.of("key", 1));
        expected.put("value", null);

        // When / Then
        assertThat(serDe.fromJson("{\"key\":1,\"value\":null}"))
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"key\":null,\"sort\":null}"})
    void shouldOmitMissingOrNullNonNullableKeys(String json) {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .sortKeyFields(new Field("sort", new StringType()))
                .build();
        RowJsonSerDe serDe = new RowJsonSerDe(schema);

        // When / Then
        assertThat(serDe.fromJson(json)).isEqualTo(new Row());
    }

    private RowJsonSerDe serDe(Type type, boolean nullable) {
        return new RowJsonSerDe(Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", type, nullable))
                .build());
    }

    private static Stream<Type> valueTypes() {
        return Stream.of(new IntType(), new LongType(), new StringType(), new ByteArrayType(),
                new ListType(new StringType()), new MapType(new StringType(), new LongType()));
    }
}
