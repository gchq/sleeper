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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

public class RangeFactoryTest {
    
    @Test
    public void shouldCreateCorrectRangesForIntKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        
        // When
        Range range1 = rangeFactory.createRange(field, 1, true, 10, true);
        Range range2 = rangeFactory.createRange(field, 1, true, 10, false);
        Range range3 = rangeFactory.createRange(field, 1, false, 10, true);
        Range range4 = rangeFactory.createRange(field, 1, false, 10, false);
        Range range5 = rangeFactory.createRange(field, Integer.MIN_VALUE, true, null, true);
        Range range6 = rangeFactory.createExactRange(field, 1);
        
        // Then
        assertEquals(1, range1.getMin());
        assertTrue(range1.isMinInclusive());
        assertEquals(10, range1.getMax());
        assertTrue(range1.isMaxInclusive());
        assertEquals(1, range2.getMin());
        assertTrue(range2.isMinInclusive());
        assertEquals(10, range2.getMax());
        assertFalse(range2.isMaxInclusive());
        assertEquals(1, range3.getMin());
        assertFalse(range3.isMinInclusive());
        assertEquals(10, range3.getMax());
        assertTrue(range3.isMaxInclusive());
        assertEquals(1, range4.getMin());
        assertFalse(range4.isMinInclusive());
        assertEquals(10, range4.getMax());
        assertFalse(range4.isMinInclusive());
        assertEquals(Integer.MIN_VALUE, range5.getMin());
        assertTrue(range5.isMinInclusive()); // If max is null, maxInclusive should be set to false
        assertEquals(null, range5.getMax());
        assertFalse(range4.isMaxInclusive());
        assertEquals(1, range6.getMin());
        assertTrue(range6.isMinInclusive());
        assertEquals(1, range6.getMax());
        assertTrue(range6.isMaxInclusive());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptNullMin() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new StringType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        
        // When / Then
        rangeFactory.createRange(field, null, false, "A", true);
    }
}
