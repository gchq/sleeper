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
package sleeper.sketches;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SketchesSerDeTest {

    @Test
    void shouldSerDeIntType() throws Exception {
        Schema schema = createSchemaWithKey("key", new IntType());
        Sketches sketches = Sketches.from(schema);
        for (int i = 0; i < 100; i++) {
            sketches.update(new Row(Map.of("key", i)));
        }

        // When
        byte[] bytes = serialise(sketches, schema);
        Sketches deserialised = deserialise(bytes, schema);

        // Then
        assertThat(SketchesDeciles.from(deserialised)).isEqualTo(SketchesDeciles.builder()
                .field("key", deciles -> deciles
                        .min(0).max(99)
                        .rank(0.1, 10).rank(0.2, 20).rank(0.3, 30)
                        .rank(0.4, 40).rank(0.5, 50).rank(0.6, 60)
                        .rank(0.7, 70).rank(0.8, 80).rank(0.9, 90))
                .build());
    }

    @Test
    void shouldSerDeLongType() throws Exception {
        Schema schema = createSchemaWithKey("key", new LongType());
        Sketches sketches = Sketches.from(schema);
        for (long i = 0; i < 100; i++) {
            sketches.update(new Row(Map.of("key", i)));
        }

        // When
        byte[] bytes = serialise(sketches, schema);
        Sketches deserialised = deserialise(bytes, schema);

        // Then
        assertThat(SketchesDeciles.from(deserialised)).isEqualTo(SketchesDeciles.builder()
                .field("key", deciles -> deciles
                        .min(0L).max(99L)
                        .rank(0.1, 10L).rank(0.2, 20L).rank(0.3, 30L)
                        .rank(0.4, 40L).rank(0.5, 50L).rank(0.6, 60L)
                        .rank(0.7, 70L).rank(0.8, 80L).rank(0.9, 90L))
                .build());
    }

    @Test
    void shouldSerDeStringType() throws Exception {
        Schema schema = createSchemaWithKey("key", new StringType());
        Sketches sketches = Sketches.from(schema);
        for (int i = 0; i < 100; i++) {
            sketches.update(new Row(Map.of("key", "" + i)));
        }

        // When
        byte[] bytes = serialise(sketches, schema);
        Sketches deserialised = deserialise(bytes, schema);

        // Then
        assertThat(SketchesDeciles.from(deserialised)).isEqualTo(SketchesDeciles.builder()
                .field("key", deciles -> deciles
                        .min("0").max("99")
                        .rank(0.1, "18").rank(0.2, "27").rank(0.3, "36")
                        .rank(0.4, "45").rank(0.5, "54").rank(0.6, "63")
                        .rank(0.7, "72").rank(0.8, "81").rank(0.9, "90"))
                .build());
    }

    @Test
    void shouldSerDeByteArrayType() throws Exception {
        Schema schema = createSchemaWithKey("key", new ByteArrayType());
        Sketches sketches = Sketches.from(schema);
        for (byte i = 0; i < 100; i++) {
            sketches.update(new Row(Map.of("key", new byte[]{i, (byte) (i + 1)})));
        }

        // When
        byte[] bytes = serialise(sketches, schema);
        Sketches deserialised = deserialise(bytes, schema);

        // Then
        assertThat(SketchesDeciles.from(deserialised)).isEqualTo(SketchesDeciles.builder()
                .field("key", deciles -> deciles
                        .minBytes(0, 1).maxBytes(99, 100)
                        .rankBytes(0.1, 10, 11).rankBytes(0.2, 20, 21).rankBytes(0.3, 30, 31)
                        .rankBytes(0.4, 40, 41).rankBytes(0.5, 50, 51).rankBytes(0.6, 60, 61)
                        .rankBytes(0.7, 70, 71).rankBytes(0.8, 80, 81).rankBytes(0.9, 90, 91))
                .build());
    }

    @Test
    public void shouldSerDeMultipleFields() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new IntType()),
                new Field("key2", new LongType()),
                new Field("key3", new StringType()),
                new Field("key4", new ByteArrayType()))
                .build();
        Sketches sketches = Sketches.from(schema);
        for (int i = 0; i < 100; i++) {
            sketches.update(new Row(Map.of(
                    "key1", i,
                    "key2", i + 1_000_000L,
                    "key3", "" + (i + 1_000_000L),
                    "key4", new byte[]{(byte) i, (byte) (i + 1)})));
        }

        // When
        byte[] bytes = serialise(sketches, schema);
        Sketches deserialised = deserialise(bytes, schema);

        // Then
        assertThat(SketchesDeciles.from(deserialised)).isEqualTo(SketchesDeciles.builder()
                .field("key1", deciles -> deciles
                        .min(0).max(99)
                        .rank(0.1, 10).rank(0.2, 20).rank(0.3, 30)
                        .rank(0.4, 40).rank(0.5, 50).rank(0.6, 60)
                        .rank(0.7, 70).rank(0.8, 80).rank(0.9, 90))
                .field("key2", deciles -> deciles
                        .min(1_000_000L).max(1_000_099L)
                        .rank(0.1, 1_000_010L).rank(0.2, 1_000_020L).rank(0.3, 1_000_030L)
                        .rank(0.4, 1_000_040L).rank(0.5, 1_000_050L).rank(0.6, 1_000_060L)
                        .rank(0.7, 1_000_070L).rank(0.8, 1_000_080L).rank(0.9, 1_000_090L))
                .field("key3", deciles -> deciles
                        .min("1000000").max("1000099")
                        .rank(0.1, "1000010").rank(0.2, "1000020").rank(0.3, "1000030")
                        .rank(0.4, "1000040").rank(0.5, "1000050").rank(0.6, "1000060")
                        .rank(0.7, "1000070").rank(0.8, "1000080").rank(0.9, "1000090"))
                .field("key4", deciles -> deciles
                        .minBytes(0, 1).maxBytes(99, 100)
                        .rankBytes(0.1, 10, 11).rankBytes(0.2, 20, 21).rankBytes(0.3, 30, 31)
                        .rankBytes(0.4, 40, 41).rankBytes(0.5, 50, 51).rankBytes(0.6, 60, 61)
                        .rankBytes(0.7, 70, 71).rankBytes(0.8, 80, 81).rankBytes(0.9, 90, 91))
                .build());
    }

    @Test
    void shouldSerDeNulls() throws Exception {
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new IntType()),
                new Field("key2", new LongType()),
                new Field("key3", new StringType()),
                new Field("key4", new ByteArrayType()))
                .build();
        Sketches sketches = Sketches.from(schema);
        sketches.update(new Row());

        // When
        byte[] bytes = serialise(sketches, schema);
        Sketches deserialised = deserialise(bytes, schema);

        // Then
        assertThat(SketchesDeciles.from(deserialised)).isEqualTo(SketchesDeciles.builder()
                .fieldEmpty("key1")
                .fieldEmpty("key2")
                .fieldEmpty("key3")
                .fieldEmpty("key4")
                .build());
    }

    private static byte[] serialise(Sketches sketches, Schema schema) throws Exception {
        return new SketchesSerDe(schema).toBytes(sketches);
    }

    private static Sketches deserialise(byte[] bytes, Schema schema) throws Exception {
        return new SketchesSerDe(schema).fromBytes(bytes);
    }
}
