/*
 * Copyright 2022-2024 Crown Copyright
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

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SketchSerialiserTest {

    @Test
    public void shouldSerDe() throws IOException {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new LongType());
        Field field3 = new Field("key3", new StringType());
        Field field4 = new Field("key4", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2, field3, field4).build();
        ItemsSketch<Integer> sketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        for (int i = 0; i < 100; i++) {
            sketch1.update(i);
        }
        ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        for (long i = 1_000_000L; i < 1_000_500L; i++) {
            sketch2.update(i);
        }
        ItemsSketch<String> sketch3 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        for (long i = 1_000_000L; i < 1_000_500L; i++) {
            sketch3.update("" + i);
        }
        ItemsSketch<ByteArray> sketch4 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        for (byte i = 0; i < 100; i++) {
            sketch4.update(ByteArray.wrap(new byte[]{i, (byte) (i + 1)}));
        }
        Map<String, ItemsSketch> map = new HashMap<>();
        map.put("key1", sketch1);
        map.put("key2", sketch2);
        map.put("key3", sketch3);
        map.put("key4", sketch4);
        Sketches sketches = new Sketches(map);
        SketchSerialiser sketchSerialiser = new SketchSerialiser(schema);

        // When
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        sketchSerialiser.serialise(sketches, dos);
        byte[] serialisedSketch = baos.toByteArray();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialisedSketch));
        Sketches deserialisedSketches = sketchSerialiser.deserialise(dis);

        // Then
        assertThat(SketchesDeciles.from(deserialisedSketches)).isEqualTo(SketchesDeciles.builder()
                .field("key1", builder -> builder
                        .min(0).max(99)
                        .rank(0.1, 10).rank(0.2, 20).rank(0.3, 30)
                        .rank(0.4, 40).rank(0.5, 50).rank(0.6, 60)
                        .rank(0.7, 70).rank(0.8, 80).rank(0.9, 90))
                .field("key2", builder -> builder
                        .min(1_000_000L).max(1_000_499L)
                        .rank(0.1, 1_000_050L).rank(0.2, 1_000_100L).rank(0.3, 1_000_150L)
                        .rank(0.4, 1_000_200L).rank(0.5, 1_000_250L).rank(0.6, 1_000_300L)
                        .rank(0.7, 1_000_350L).rank(0.8, 1_000_400L).rank(0.9, 1_000_450L))
                .field("key3", builder -> builder
                        .min("1000000").max("1000499")
                        .rank(0.1, "1000050").rank(0.2, "1000100").rank(0.3, "1000150")
                        .rank(0.4, "1000200").rank(0.5, "1000250").rank(0.6, "1000300")
                        .rank(0.7, "1000350").rank(0.8, "1000400").rank(0.9, "1000450"))
                .field("key4", builder -> builder
                        .minBytes(0, 1).maxBytes(99, 100)
                        .rankBytes(0.1, 10, 11).rankBytes(0.2, 20, 21).rankBytes(0.3, 30, 31)
                        .rankBytes(0.4, 40, 41).rankBytes(0.5, 50, 51).rankBytes(0.6, 60, 61)
                        .rankBytes(0.7, 70, 71).rankBytes(0.8, 80, 81).rankBytes(0.9, 90, 91))
                .build());
    }
}
