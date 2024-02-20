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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SketchSerialiserTest {

    @SuppressWarnings("unchecked")
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
        assertThat(deserialisedSketches.getQuantilesSketches().keySet()).isEqualTo(new HashSet<>(schema.getRowKeyFieldNames()));
        for (Map.Entry<String, ItemsSketch> entry : map.entrySet()) {
            assertThat(deserialisedSketches.getQuantilesSketch(entry.getKey()).getMinValue()).isEqualTo(entry.getValue().getMinValue());
            assertThat(deserialisedSketches.getQuantilesSketch(entry.getKey()).getMaxValue()).isEqualTo(entry.getValue().getMaxValue());
            for (double d = 0.0D; d < 1.0D; d += 0.1D) {
                assertThat(deserialisedSketches.getQuantilesSketch(entry.getKey()).getQuantile(d)).isEqualTo(entry.getValue().getQuantile(d));
            }
        }
    }
}
