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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

public class RangeTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Field field = new Field("key", new IntType());
        Range range1 = new Range(field, 10, true, 20, true);
        Range range2 = new Range(field, 10, true, 20, true);
        Range range3 = new Range(field, 10, true, 20, false);
        Range range4 = new Range(field, 11, true, 20, false);
        
        // When
        boolean equals1 = range1.equals(range2);
        boolean equals2 = range1.equals(range3);
        boolean equals3 = range3.equals(range4);
        int hashCode1 = range1.hashCode();
        int hashCode2 = range2.hashCode();
        int hashCode3 = range3.hashCode();
        int hashCode4 = range4.hashCode();
        
        // Then
        assertTrue(equals1);
        assertFalse(equals2);
        assertFalse(equals3);
        assertEquals(hashCode1, hashCode2);
        assertNotEquals(hashCode1, hashCode3);
        assertNotEquals(hashCode3, hashCode4);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfGivenWrongType() {
        // Given
        Field field = new Field("key", new IntType());
        Range range = new Range(field, 10, true, 20, true);
        
        // When / Then
        range.doesRangeContainObject(10L);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeBothInclusive() {
        // Given
        Field field = new Field("key", new IntType());
        Range range = new Range(field, 10, true, 20, true);
        
        // When
        boolean test1 = range.doesRangeContainObject(10);
        boolean test2 = range.doesRangeContainObject(15);
        boolean test3 = range.doesRangeContainObject(20);
        boolean test4 = range.doesRangeContainObject(0);
        boolean test5 = range.doesRangeContainObject(21);
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeBothExclusive() {
        // Given
        Field field = new Field("key", new IntType());
        Range range = new Range(field, 10, false, 20, false);
        
        // When
        boolean test1 = range.doesRangeContainObject(10);
        boolean test2 = range.doesRangeContainObject(15);
        boolean test3 = range.doesRangeContainObject(20);
        boolean test4 = range.doesRangeContainObject(0);
        boolean test5 = range.doesRangeContainObject(21);
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeMinInclusiveMaxExclusive() {
        // Given
        Field field = new Field("key", new IntType());
        Range range = new Range(field, 10, true, 20, false);
        
        // When
        boolean test1 = range.doesRangeContainObject(10);
        boolean test2 = range.doesRangeContainObject(15);
        boolean test3 = range.doesRangeContainObject(20);
        boolean test4 = range.doesRangeContainObject(0);
        boolean test5 = range.doesRangeContainObject(21);
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyIntRangeMinExclusiveMaxInclusive() {
        // Given
        Field field = new Field("key", new IntType());
        Range range = new Range(field, 10, false, 20, true);
        
        // When
        boolean test1 = range.doesRangeContainObject(10);
        boolean test2 = range.doesRangeContainObject(15);
        boolean test3 = range.doesRangeContainObject(20);
        boolean test4 = range.doesRangeContainObject(0);
        boolean test5 = range.doesRangeContainObject(21);
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeBothInclusive() {
        // Given
        Field field = new Field("key", new LongType());
        Range range = new Range(field, 10L, true, 20L, true);
        
        // When
        boolean test1 = range.doesRangeContainObject(10L);
        boolean test2 = range.doesRangeContainObject(15L);
        boolean test3 = range.doesRangeContainObject(20L);
        boolean test4 = range.doesRangeContainObject(0L);
        boolean test5 = range.doesRangeContainObject(21L);
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeBothExclusive() {
        // Given
        Field field = new Field("key", new LongType());
        Range range = new Range(field, 10L, false, 20L, false);
        
        // When
        boolean test1 = range.doesRangeContainObject(10L);
        boolean test2 = range.doesRangeContainObject(15L);
        boolean test3 = range.doesRangeContainObject(20L);
        boolean test4 = range.doesRangeContainObject(0L);
        boolean test5 = range.doesRangeContainObject(21L);
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeMinInclusiveMaxExclusive() {
        // Given
        Field field = new Field("key", new LongType());
        Range range = new Range(field, 10L, true, 20L, false);
        
        // When
        boolean test1 = range.doesRangeContainObject(10L);
        boolean test2 = range.doesRangeContainObject(15L);
        boolean test3 = range.doesRangeContainObject(20L);
        boolean test4 = range.doesRangeContainObject(0L);
        boolean test5 = range.doesRangeContainObject(21L);
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyLongRangeMinExclusiveMaxInclusive() {
        // Given
        Field field = new Field("key", new LongType());
        Range range = new Range(field, 10L, false, 20L, true);
        
        // When
        boolean test1 = range.doesRangeContainObject(10L);
        boolean test2 = range.doesRangeContainObject(15L);
        boolean test3 = range.doesRangeContainObject(20L);
        boolean test4 = range.doesRangeContainObject(0L);
        boolean test5 = range.doesRangeContainObject(21L);
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeBothInclusive() {
        // Given
        Field field = new Field("key", new StringType());
        Range range = new Range(field, "B", true, "G", true);
        
        // When
        boolean test1 = range.doesRangeContainObject("B");
        boolean test2 = range.doesRangeContainObject("E");
        boolean test3 = range.doesRangeContainObject("G");
        boolean test4 = range.doesRangeContainObject("A");
        boolean test5 = range.doesRangeContainObject("H");
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeBothExclusive() {
        // Given
        Field field = new Field("key", new StringType());
        Range range = new Range(field, "B", false, "G", false);
        
        // When
        boolean test1 = range.doesRangeContainObject("B");
        boolean test2 = range.doesRangeContainObject("E");
        boolean test3 = range.doesRangeContainObject("G");
        boolean test4 = range.doesRangeContainObject("A");
        boolean test5 = range.doesRangeContainObject("H");
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeMinInclusiveMaxExclusive() {
        // Given
        Field field = new Field("key", new StringType());
        Range range = new Range(field, "B", true, "G", false);
        
        // When
        boolean test1 = range.doesRangeContainObject("B");
        boolean test2 = range.doesRangeContainObject("E");
        boolean test3 = range.doesRangeContainObject("G");
        boolean test4 = range.doesRangeContainObject("A");
        boolean test5 = range.doesRangeContainObject("H");
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyStringRangeMinExclusiveMaxInclusive() {
        // Given
        Field field = new Field("key", new StringType());
        Range range = new Range(field, "B", false, "G", true);
        
        // When
        boolean test1 = range.doesRangeContainObject("B");
        boolean test2 = range.doesRangeContainObject("E");
        boolean test3 = range.doesRangeContainObject("G");
        boolean test4 = range.doesRangeContainObject("A");
        boolean test5 = range.doesRangeContainObject("H");
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeBothInclusive() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Range range = new Range(field, new byte[]{10, 10}, true, new byte[]{20, 20}, true);
        
        // When
        boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
        boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
        boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
        boolean test4 = range.doesRangeContainObject(new byte[]{9});
        boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeBothExclusive() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Range range = new Range(field, new byte[]{10, 10}, false, new byte[]{20, 20}, false);
        
        // When
        boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
        boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
        boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
        boolean test4 = range.doesRangeContainObject(new byte[]{9});
        boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeMinInclusiveMaxExclusive() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Range range = new Range(field, new byte[]{10, 10}, true, new byte[]{20, 20}, false);
        
        // When
        boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
        boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
        boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
        boolean test4 = range.doesRangeContainObject(new byte[]{9});
        boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});
        
        // Then
        assertTrue(test1);
        assertTrue(test2);
        assertFalse(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldAnswerDoesRangeContainObjectCorreclyByteArrayRangeMinExclusiveMaxInclusive() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Range range = new Range(field, new byte[]{10, 10}, false, new byte[]{20, 20}, true);
        
        // When
        boolean test1 = range.doesRangeContainObject(new byte[]{10, 10});
        boolean test2 = range.doesRangeContainObject(new byte[]{15, 16, 17});
        boolean test3 = range.doesRangeContainObject(new byte[]{20, 20});
        boolean test4 = range.doesRangeContainObject(new byte[]{9});
        boolean test5 = range.doesRangeContainObject(new byte[]{20, 21});
        
        // Then
        assertFalse(test1);
        assertTrue(test2);
        assertTrue(test3);
        assertFalse(test4);
        assertFalse(test5);
    }
    
    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapWithIntKey() {
        // Given
        Field field = new Field("key1", new IntType());
        Range range = new Range(field, 1, true, 10, false);
        
        // When
        //  - Other range is in canonical form, i.e. inclusive of min, exclusive of max
        boolean does1To2OverlapPartition = range.doesRangeOverlap(new Range(field, 1, 2));
        boolean does4To5OverlapPartition = range.doesRangeOverlap(new Range(field, 4, 5));
        boolean does1To10OverlapPartition = range.doesRangeOverlap(new Range(field, 1, 10));
        boolean doesMinus5To10OverlapPartition = range.doesRangeOverlap(new Range(field, -5, 10));
        boolean doesMinus5To15OverlapPartition = range.doesRangeOverlap(new Range(field, -5, 15));
        boolean doesMinus5To1OverlapPartition = range.doesRangeOverlap(new Range(field, -5, 1));
        boolean doesMinus10ToMinus5OverlapPartition = range.doesRangeOverlap(new Range(field, -10, -5));
        boolean does10To100OverlapPartition = range.doesRangeOverlap(new Range(field, 10, 100));
        //  - Inclusive of min, inclusive of max
        boolean does1To10IncOverlapPartition = range.doesRangeOverlap(new Range(field, 1, true, 10, true));
        boolean does10To11IncOverlapPartition = range.doesRangeOverlap(new Range(field, 10, true, 11, true));
        boolean doesMinus1To1IncOverlapPartition = range.doesRangeOverlap(new Range(field, -1, true, 1, true));
        //  - Exclusive of min, exclusive of max
        boolean does1To10ExcOverlapPartition = range.doesRangeOverlap(new Range(field, 1, false, 10, false));
        boolean does9To11ExcOverlapPartition = range.doesRangeOverlap(new Range(field, 9, false, 11, false));
        //  - Exclusive of min, inclusive of max
        boolean does9To11ExcIncOverlapPartition = range.doesRangeOverlap(new Range(field, 9, false, 11, true));
        
        // Then
        assertTrue(does1To2OverlapPartition);
        assertTrue(does4To5OverlapPartition);
        assertTrue(does1To10OverlapPartition);
        assertTrue(doesMinus5To10OverlapPartition);
        assertTrue(doesMinus5To15OverlapPartition);
        assertFalse(doesMinus5To1OverlapPartition);
        assertFalse(doesMinus10ToMinus5OverlapPartition);
        assertFalse(does10To100OverlapPartition);
        assertTrue(does1To10IncOverlapPartition);
        assertFalse(does10To11IncOverlapPartition);
        assertTrue(doesMinus1To1IncOverlapPartition);
        assertTrue(does1To10ExcOverlapPartition);
        assertFalse(does9To11ExcOverlapPartition);
        assertFalse(does9To11ExcIncOverlapPartition);
    }

    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithLongKey() {
        // Given
        Field field = new Field("key1", new LongType());
        Range range = new Range(field, 1L, true, 10L, false);

        // When
        //  - Other range is in canonical form, i.e. inclusive of min, exclusive of max
        boolean does1To2OverlapPartition = range.doesRangeOverlap(new Range(field, 1L, 2L));
        boolean does4To5OverlapPartition = range.doesRangeOverlap(new Range(field, 4L, 5L));
        boolean does1To10OverlapPartition = range.doesRangeOverlap(new Range(field, 1L, 10L));
        boolean doesMinus5To10OverlapPartition = range.doesRangeOverlap(new Range(field, -5L, 10L));
        boolean doesMinus5To15OverlapPartition = range.doesRangeOverlap(new Range(field, -5L, 15L));
        boolean doesMinus5To1OverlapPartition = range.doesRangeOverlap(new Range(field, -5L, 1L));
        boolean doesMinus10ToMinus5OverlapPartition = range.doesRangeOverlap(new Range(field, -10L, -5L));
        boolean does10To100OverlapPartition = range.doesRangeOverlap(new Range(field, 10L, 100L));
        //  - Inclusive of min, inclusive of max
        boolean does1To10IncOverlapPartition = range.doesRangeOverlap(new Range(field, 1L, true, 10L, true));
        boolean does10To11IncOverlapPartition = range.doesRangeOverlap(new Range(field, 10L, true, 11L, true));
        boolean doesMinus1To1IncOverlapPartition = range.doesRangeOverlap(new Range(field, -1L, true, 1L, true));
        //  - Exclusive of min, exclusive of max
        boolean does1To10ExcOverlapPartition = range.doesRangeOverlap(new Range(field, 1L, false, 10L, false));
        boolean does9To11ExcOverlapPartition = range.doesRangeOverlap(new Range(field, 9L, false, 11L, false));
        //  - Exclusive of min, inclusive of max
        boolean does9To11ExcIncOverlapPartition = range.doesRangeOverlap(new Range(field, 9L, false, 11L, true));
            
        // Then
        assertTrue(does1To2OverlapPartition);
        assertTrue(does4To5OverlapPartition);
        assertTrue(does1To10OverlapPartition);
        assertTrue(doesMinus5To10OverlapPartition);
        assertTrue(doesMinus5To15OverlapPartition);
        assertFalse(doesMinus5To1OverlapPartition);
        assertFalse(doesMinus10ToMinus5OverlapPartition);
        assertFalse(does10To100OverlapPartition);
        assertTrue(does1To10IncOverlapPartition);
        assertFalse(does10To11IncOverlapPartition);
        assertTrue(doesMinus1To1IncOverlapPartition);
        assertTrue(does1To10ExcOverlapPartition);
        assertFalse(does9To11ExcOverlapPartition);
        assertFalse(does9To11ExcIncOverlapPartition);
    }
    
    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithStringKey() {
        // Given
        Field field = new Field("key1", new StringType());
        Range range = new Range(field, "B", true, "G", false);

        // When
        //  - Other range is in canonical form, i.e. inclusive of min, exclusive of max
        boolean doesBToCOverlapPartition = range.doesRangeOverlap(new Range(field, "B", "C"));
        boolean doesCToDOverlapPartition = range.doesRangeOverlap(new Range(field, "C", "D"));
        boolean doesBToGOverlapPartition = range.doesRangeOverlap(new Range(field, "B", "G"));
        boolean doesAToDOverlapPartition = range.doesRangeOverlap(new Range(field, "A", "D"));
        boolean doesDToJOverlapPartition = range.doesRangeOverlap(new Range(field, "D", "J"));
        boolean doesAToBOverlapPartition = range.doesRangeOverlap(new Range(field, "A", "B"));
        boolean doesA1ToA2OverlapPartition = range.doesRangeOverlap(new Range(field, "A1", "A2"));
        boolean doesGToZOverlapPartition = range.doesRangeOverlap(new Range(field, "G", "Z"));
        //  - Inclusive of min, inclusive of max
        boolean doesBToGIncOverlapPartition = range.doesRangeOverlap(new Range(field, "B", true, "G", true));
        boolean doesGToHIncOverlapPartition = range.doesRangeOverlap(new Range(field, "G", true, "H", true));
        boolean doesAToBIncOverlapPartition = range.doesRangeOverlap(new Range(field, "A", true, "B", true));
        //  - Exclusive of min, exclusive of max
        boolean doesBToGExcOverlapPartition = range.doesRangeOverlap(new Range(field, "B", false, "G", false));
        boolean doesFToHExcOverlapPartition = range.doesRangeOverlap(new Range(field, "F", false, "H", false));
        //  - Exclusive of min, inclusive of max
        boolean doesFToHExcIncOverlapPartition = range.doesRangeOverlap(new Range(field, "F", false, "H", true));
        
        // Then
        assertTrue(doesBToCOverlapPartition);
        assertTrue(doesCToDOverlapPartition);
        assertTrue(doesBToGOverlapPartition);
        assertTrue(doesAToDOverlapPartition);
        assertTrue(doesDToJOverlapPartition);
        assertFalse(doesAToBOverlapPartition);
        assertFalse(doesA1ToA2OverlapPartition);
        assertFalse(doesGToZOverlapPartition);
        assertTrue(doesBToGIncOverlapPartition);
        assertFalse(doesGToHIncOverlapPartition);
        assertTrue(doesAToBIncOverlapPartition);
        assertTrue(doesBToGExcOverlapPartition);
        assertTrue(doesFToHExcOverlapPartition);
        assertTrue(doesFToHExcIncOverlapPartition);
    }
    
    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithByteArrayKey() {
        // Given
        Field field = new Field("key1", new ByteArrayType());
        Range range = new Range(field, new byte[]{1, 64}, true, new byte[]{1, 88}, false);

        // When
        boolean does164To165OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 64}, new byte[]{1, 65}));
        boolean does164To168OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 64}, new byte[]{1, 68}));
        boolean does164To188OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 64}, new byte[]{1, 88}));
        boolean does0To170OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{0}, new byte[]{1, 70}));
        boolean does170To299OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 70}, new byte[]{2, 99}));
        boolean does0To164OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{0}, new byte[]{1, 64}));
        boolean does11To11OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 1}, new byte[]{1, 1}));
        boolean does188To9999OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 88}, new byte[]{99, 99}));
        //  - Inclusive of min, inclusive of max
        boolean does164To188IncOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 64}, true, new byte[]{1, 88}, true));
        boolean does188To2IncOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 88}, true, new byte[]{2}, true));
        boolean does0To164IncOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{0}, true, new byte[]{1, 64}, true));
        //  - Exclusive of min, exclusive of max
        boolean does164To188ExcOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 64}, false, new byte[]{1, 88}, false));
        boolean does188To2ExcOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 88}, false, new byte[]{2}, false));
        //  - Exclusive of min, inclusive of max
        boolean does187To2ExcIncOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{1, 88}, false, new byte[]{2}, true));

        // Then
        assertTrue(does164To165OverlapPartition);
        assertTrue(does164To168OverlapPartition);
        assertTrue(does164To188OverlapPartition);
        assertTrue(does0To170OverlapPartition);
        assertTrue(does170To299OverlapPartition);
        assertFalse(does0To164OverlapPartition);
        assertFalse(does11To11OverlapPartition);
        assertFalse(does188To9999OverlapPartition);
        assertTrue(does164To188IncOverlapPartition);
        assertFalse(does188To2IncOverlapPartition);
        assertTrue(does0To164IncOverlapPartition);
        assertTrue(does164To188ExcOverlapPartition);
        assertFalse(does188To2ExcOverlapPartition);
        assertFalse(does187To2ExcIncOverlapPartition);
    }
    
    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithStringKeyAndEmptyStringAndNullBoundaries() {
        // Given
        Field field = new Field("key1", new StringType());
        Range range = new Range(field, "", true, null, false);

        // When
        boolean doesCToDOverlapPartition = range.doesRangeOverlap(new Range(field, "C", "D"));
        boolean doesEmptyToZOverlapPartition = range.doesRangeOverlap(new Range(field, "", "Z"));
        boolean doesXToNullOverlapPartition = range.doesRangeOverlap(new Range(field, "X", null));
        //  - Inclusive of min, inclusive of max
        boolean doesBToBOverlapPartition = range.doesRangeOverlap(new Range(field, "B", true, "B", true));
        boolean doesEmptyStringToNullOverlapPartition = range.doesRangeOverlap(new Range(field, "", true, null, true));
        //  - Exclusive of min, exclusive of max
        boolean doesEmptyStringToEmptyExcStringOverlapPartition = range.doesRangeOverlap(new Range(field, "", false, "B", false));
        //  - Exclusive of min, inclusive of max
        boolean doesEmptyStringToBExcIncOverlapPartition = range.doesRangeOverlap(new Range(field, "", false, "B", true));
        
        // Then
        assertTrue(doesCToDOverlapPartition);
        assertTrue(doesEmptyToZOverlapPartition);
        assertTrue(doesXToNullOverlapPartition);
        assertTrue(doesBToBOverlapPartition);
        assertTrue(doesEmptyStringToNullOverlapPartition);
        assertTrue(doesEmptyStringToEmptyExcStringOverlapPartition);
        assertTrue(doesEmptyStringToBExcIncOverlapPartition);
    }

    @Test
    public void shouldGiveCorrectAnswerForDoesRangeOverlapPartitionWithByteArrayKeyAndEmptyByteArrayAndNullBoundaries() {
        // Given
        Field field = new Field("key1", new ByteArrayType());
        Range range = new Range(field, new byte[]{}, null);

        // When
        boolean does5To5OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{5}, true, new byte[]{5}, true));
        boolean does5To77OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{5}, true, new byte[]{7, 7}, false));
        boolean doesEmptyTo888OverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{}, true, new byte[]{8, 8, 8}, false));
        boolean does888ToNullOverlapPartition = range.doesRangeOverlap(new Range(field, new byte[]{8, 8, 8}, true, null, false));
        
        // Then
        assertTrue(does5To5OverlapPartition);
        assertTrue(does5To77OverlapPartition);
        assertTrue(doesEmptyTo888OverlapPartition);
        assertTrue(does888ToNullOverlapPartition);
    }
}