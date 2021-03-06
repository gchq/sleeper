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
package sleeper.configuration;

import org.junit.Test;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class EstimateSplitPointsTest {

    @Test
    public void shouldEstimateCorrectlyWithIntKey() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new IntType()));
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", 100 - i - 1); // Reverse order because the method shouldn't assume that the records are sorted
            records.add(record);
        }
        EstimateSplitPoints estimateSplitPoints = new EstimateSplitPoints(schema, records, 10);

        // When
        List<Object> splitPoints = estimateSplitPoints.estimate();

        // Then
        List<Object> expected = Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90);
        assertEquals(expected, splitPoints);
    }

    @Test
    public void shouldEstimateCorrectlyWithLongKey() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i * 100L);
            records.add(record);
        }
        EstimateSplitPoints estimateSplitPoints = new EstimateSplitPoints(schema, records, 10);

        // When
        List<Object> splitPoints = estimateSplitPoints.estimate();

        // Then
        List<Object> expected = Arrays.asList(1000L, 2000L, 3000L, 4000L, 5000L, 6000L, 7000L, 8000L, 9000L);
        assertEquals(expected, splitPoints);
    }

    @Test
    public void shouldEstimateCorrectlyWithStringKey() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new StringType()));
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", String.format ("%04d", i * 100L));
            records.add(record);
        }
        EstimateSplitPoints estimateSplitPoints = new EstimateSplitPoints(schema, records, 10);

        // When
        List<Object> splitPoints = estimateSplitPoints.estimate();

        // Then
        List<Object> expected = Arrays.asList("1000", "2000", "3000", "4000", "5000", "6000", "7000", "8000", "9000");
        assertEquals(expected, splitPoints);
    }

    @Test
    public void shouldEstimateCorrectlyWithByteArrayKey() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", new byte[]{(byte) i});
            records.add(record);
        }
        EstimateSplitPoints estimateSplitPoints = new EstimateSplitPoints(schema, records, 10);

        // When
        List<Object> splitPoints = estimateSplitPoints.estimate();

        // Then
        List<Object> expected = Arrays.asList(new byte[]{(byte) 10},
                new byte[]{(byte) 20}, new byte[]{(byte) 30}, new byte[]{(byte) 40}, new byte[]{(byte) 50},
                new byte[]{(byte) 60}, new byte[]{(byte) 70}, new byte[]{(byte) 80}, new byte[]{(byte) 90});
        assertEquals(expected.size(), splitPoints.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals((byte[]) expected.get(i), (byte[]) splitPoints.get(i));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRefuseToSplitIntoOnePartition() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", new byte[]{(byte) i});
            records.add(record);
        }
        EstimateSplitPoints estimateSplitPoints = new EstimateSplitPoints(schema, records, 1);

        // When / Then
        estimateSplitPoints.estimate();
    }
}
