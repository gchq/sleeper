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

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.MapType;
import sleeper.core.schema.type.StringType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RowConverterTest {

    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value", new StringType(), true))
            .build();
    // Field indices: key=0, value=1
    private final RowConverter converter = new RowConverter(schema);

    @Test
    void shouldSetNullableFieldToNullWhenNotPresent() {
        // Given
        converter.start();
        writeString(0, "row1");

        // When
        converter.end();

        // Then
        assertThat(converter.getRow().get("key")).isEqualTo("row1");
        assertThat(converter.getRow().get("value")).isNull();
    }

    @Test
    void shouldSetNullableFieldWhenValuePresent() {
        // Given
        converter.start();
        writeString(0, "row1");
        writeString(1, "hello");

        // When
        converter.end();

        // Then
        assertThat(converter.getRow().get("key")).isEqualTo("row1");
        assertThat(converter.getRow().get("value")).isEqualTo("hello");
    }

    @Test
    void shouldResetNullableFieldToNullAtStartOfEachRow() {
        // Given
        converter.start();
        writeString(0, "row1");
        writeString(1, "hello");
        converter.end();

        // When
        converter.start();
        writeString(0, "row2");
        converter.end();

        // Then
        assertThat(converter.getRow().get("key")).isEqualTo("row1");
        assertThat(converter.getRow().get("value")).isNull();
    }

    @Test
    void shouldConvertMapField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("map", new MapType(new StringType(), new StringType())))
                .build();
        RowConverter converter = new RowConverter(schema);
        converter.start();
        writeString(converter, 0, "row1");
        GroupConverter mapConverter = (GroupConverter) converter.getConverter(1);
        mapConverter.start();
        writeMapEntry(mapConverter, "k1", "v1");
        writeMapEntry(mapConverter, "k2", "v2");
        mapConverter.end();

        // When
        converter.end();

        // Then
        assertThat(converter.getRow().get("key")).isEqualTo("row1");
        assertThat(converter.getRow().get("map")).isEqualTo(Map.of("k1", "v1", "k2", "v2"));
    }

    @Test
    void shouldResetMapEntriesBetweenRows() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("map", new MapType(new StringType(), new StringType())))
                .build();
        RowConverter converter = new RowConverter(schema);
        converter.start();
        writeString(converter, 0, "row1");
        GroupConverter mapConverter = (GroupConverter) converter.getConverter(1);
        mapConverter.start();
        writeMapEntry(mapConverter, "k1", "v1");
        mapConverter.end();
        converter.end();

        // When
        converter.start();
        writeString(converter, 0, "row2");
        mapConverter.start();
        writeMapEntry(mapConverter, "k2", "v2");
        mapConverter.end();
        converter.end();

        // Then
        assertThat(converter.getRow().get("map")).isEqualTo(Map.of("k2", "v2"));
    }

    @Test
    void shouldConvertListField() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("list", new ListType(new StringType())))
                .build();
        RowConverter converter = new RowConverter(schema);
        converter.start();
        writeString(converter, 0, "row1");
        GroupConverter listConverter = (GroupConverter) converter.getConverter(1);
        listConverter.start();
        writeListElement(listConverter, "a");
        writeListElement(listConverter, "b");
        writeListElement(listConverter, "c");
        listConverter.end();

        // When
        converter.end();

        // Then
        assertThat(converter.getRow().get("key")).isEqualTo("row1");
        assertThat(converter.getRow().get("list")).isEqualTo(List.of("a", "b", "c"));
    }

    @Test
    void shouldResetListElementsBetweenRows() {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new StringType()))
                .valueFields(new Field("list", new ListType(new StringType())))
                .build();
        RowConverter converter = new RowConverter(schema);
        converter.start();
        writeString(converter, 0, "row1");
        GroupConverter listConverter = (GroupConverter) converter.getConverter(1);
        listConverter.start();
        writeListElement(listConverter, "a");
        writeListElement(listConverter, "b");
        listConverter.end();
        converter.end();

        // When
        converter.start();
        writeString(converter, 0, "row2");
        listConverter.start();
        writeListElement(listConverter, "c");
        listConverter.end();
        converter.end();

        // Then
        assertThat(converter.getRow().get("list")).isEqualTo(List.of("c"));
    }

    private void writeString(int fieldIndex, String value) {
        writeString(converter, fieldIndex, value);
    }

    private static void writeString(RowConverter converter, int fieldIndex, String value) {
        ((PrimitiveConverter) converter.getConverter(fieldIndex)).addBinary(Binary.fromString(value));
    }

    private static void writeMapEntry(GroupConverter mapConverter, String key, String value) {
        GroupConverter kvConverter = (GroupConverter) mapConverter.getConverter(0);
        kvConverter.start();
        ((PrimitiveConverter) kvConverter.getConverter(0)).addBinary(Binary.fromString(key));
        ((PrimitiveConverter) kvConverter.getConverter(1)).addBinary(Binary.fromString(value));
        kvConverter.end();
    }

    private static void writeListElement(GroupConverter listConverter, String value) {
        GroupConverter elementConverter = (GroupConverter) listConverter.getConverter(0);
        elementConverter.start();
        ((PrimitiveConverter) elementConverter.getConverter(0)).addBinary(Binary.fromString(value));
        elementConverter.end();
    }
}
