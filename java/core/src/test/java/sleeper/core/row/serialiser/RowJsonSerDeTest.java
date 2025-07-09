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
package sleeper.core.row.serialiser;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class RowJsonSerDeTest {

    @Test
    public void shouldSerDeRowWithPrimitives() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("field1", new IntType()))
                .sortKeyFields(new Field("field2", new LongType()))
                .valueFields(new Field("field3", new StringType()))
                .build();
        Row row = new Row();
        row.put("field1", 1);
        row.put("field2", 100L);
        row.put("field3", "ABC");
        RowJsonSerDe jsonSerDe = new RowJsonSerDe(schema);

        // When
        Row deserialised = jsonSerDe.fromJson(jsonSerDe.toJson(row));

        // Then
        assertThat(deserialised).isEqualTo(row);
    }

    @Test
    public void shouldSerDeRowWithByteArrays() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("field1", new ByteArrayType()))
                .sortKeyFields(new Field("field2", new ByteArrayType()))
                .valueFields(new Field("field3", new ByteArrayType()))
                .build();
        Row row = new Row();
        row.put("field1", new byte[]{});
        row.put("field2", new byte[]{1});
        row.put("field3", new byte[]{2, 3, 4});
        RowJsonSerDe jsonSerDe = new RowJsonSerDe(schema);

        // When
        Row deserialised = jsonSerDe.fromJson(jsonSerDe.toJson(row));

        // Then
        assertThat(deserialised).isEqualTo(row);
    }

    @Test
    public void shouldSerDeRowWithList() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("field1", new IntType()))
                .sortKeyFields(new Field("field2", new LongType()))
                .valueFields(new Field("field3", new ListType(new StringType())))
                .build();
        Row row = new Row();
        row.put("field1", 1);
        row.put("field2", 100L);
        row.put("field3", Arrays.asList("A", "B", "C"));
        RowJsonSerDe jsonSerDe = new RowJsonSerDe(schema);

        // When
        Row deserialised = jsonSerDe.fromJson(jsonSerDe.toJson(row));

        // Then
        assertThat(deserialised).isEqualTo(row);
    }

    @Test
    public void shouldSerDeRowWithMap() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("field1", new IntType()))
                .sortKeyFields(new Field("field2", new LongType()))
                .valueFields(new Field("field3", new MapType(new StringType(), new LongType())))
                .build();
        Row row = new Row();
        row.put("field1", 1);
        row.put("field2", 100L);
        Map<String, Long> map = new HashMap<>();
        map.put("A", 1L);
        map.put("B", 2L);
        map.put("C", 3L);
        row.put("field3", map);
        RowJsonSerDe jsonSerDe = new RowJsonSerDe(schema);

        // When
        Row deserialised = jsonSerDe.fromJson(jsonSerDe.toJson(row));

        // Then
        assertThat(deserialised).isEqualTo(row);
    }
}
