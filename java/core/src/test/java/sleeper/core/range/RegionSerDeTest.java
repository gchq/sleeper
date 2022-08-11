/*
 * Copyright 2022 Crown Copyright
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
package sleeper.core.range;

import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

public class RegionSerDeTest {

    @Test
    public void shouldSerDeCorrectlyIntKey() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, 1, minInclusive, 10, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyIntKeyNullMax() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, 1, minInclusive, null, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyLongKey() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, 1L, minInclusive, 10L, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyLongKeyNullMax() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, 1L, minInclusive, null, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyStringKey() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, "B", minInclusive, "I", maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldDeserialsieCorrectlyStringKeyBase64Encoded() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        String jsonRegion = "{\"key\":{\"min\":\"A\",\"minInclusive\":false,\"max\":\"B\",\"maxInclusive\":false},\"stringsBase64Encoded\":false}";
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        // When
        Region region = regionSerDe.fromJson(jsonRegion);

        // Then
        Range expectedRange = new Range(field, "A", false, "B", false);
        Region expectedRegion = new Region(expectedRange);
        assertThat(region).isEqualTo(expectedRegion);
    }

    @Test
    public void shouldSerDeCorrectlyStringKeyNullMax() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, "B", minInclusive, null, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyByteArrayKey() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, new byte[]{10, 11, 12}, minInclusive, new byte[]{15}, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyByteArrayKeyNullMax() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field);
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        for (boolean minInclusive : new HashSet<>(Arrays.asList(true, false))) {
            for (boolean maxInclusive : new HashSet<>(Arrays.asList(true, false))) {
                Range range = new Range(field, new byte[]{10, 11, 12}, minInclusive, null, maxInclusive);
                Region region = new Region(range);

                // When
                String serialisedRegion = regionSerDe.toJson(region);
                Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

                // Then
                assertThat(deserialisedRegion).isEqualTo(region);
            }
        }
    }

    @Test
    public void shouldSerDeCorrectlyMultipleRanges() {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new LongType());
        Field field3 = new Field("key3", new StringType());
        Field field4 = new Field("key4", new ByteArrayType());
        Schema schema = new Schema();
        schema.setRowKeyFields(field1, field2, field3, field4);
        Range range1 = new Range(field1, 1, true, 10, true);
        Range range2 = new Range(field2, 100L, true, 1000L, false);
        Range range3 = new Range(field3, "B", false, "G", true);
        Range range4 = new Range(field4, new byte[]{10, 11, 12}, false, new byte[]{15}, false);
        Region region = new Region(Arrays.asList(range1, range2, range3, range4));
        RegionSerDe regionSerDe = new RegionSerDe(schema);

        // When
        String serialisedRegion = regionSerDe.toJson(region);
        Region deserialisedRegion = regionSerDe.fromJson(serialisedRegion);

        // Then
        assertThat(deserialisedRegion).isEqualTo(region);
    }
}
