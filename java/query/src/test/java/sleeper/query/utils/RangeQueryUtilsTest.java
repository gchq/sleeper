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
package sleeper.query.utils;

import java.util.Arrays;
import java.util.List;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.And;
import org.apache.parquet.filter2.predicate.Operators.GtEq;
import org.apache.parquet.filter2.predicate.Operators.Lt;
import org.apache.parquet.filter2.predicate.Operators.Or;
import org.apache.parquet.io.api.Binary;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

public class RangeQueryUtilsTest {

    @Test
    public void shouldGiveCorrectPredicateIntKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        List<Field> rowKeyFields = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1);
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(field, 0, 100);
        Region partitionRegion = new Region(partitionRange);
        
        // When
        FilterPredicate predicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(rowKeyFields, Arrays.asList(region), partitionRegion);
        
        // Then
        assertTrue(predicate instanceof And);
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertTrue(left instanceof And);
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertTrue(leftLeft instanceof GtEq);
        assertEquals(0, ((GtEq) leftLeft).getValue());
        assertTrue(leftRight instanceof Lt);
        assertEquals(100, ((Lt) leftRight).getValue());
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertTrue(right instanceof And);
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertTrue(rightLeft instanceof GtEq);
        assertEquals(1, ((GtEq) rightLeft).getValue());
        assertTrue(rightRight instanceof Lt);
        assertEquals(2, ((Lt) rightRight).getValue());
    }

    @Test
    public void shouldGiveCorrectPredicateLongKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        List<Field> rowKeyFields = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1L);
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(field, 0L, 100L);
        Region partitionRegion = new Region(partitionRange);
        
        // When
        FilterPredicate predicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(rowKeyFields, Arrays.asList(region), partitionRegion);
        
        // Then
        assertTrue(predicate instanceof And);
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertTrue(left instanceof And);
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertTrue(leftLeft instanceof GtEq);
        assertEquals(0L, ((GtEq) leftLeft).getValue());
        assertTrue(leftRight instanceof Lt);
        assertEquals(100L, ((Lt) leftRight).getValue());
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertTrue(right instanceof And);
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertTrue(rightLeft instanceof GtEq);
        assertEquals(1L, ((GtEq) rightLeft).getValue());
        assertTrue(rightRight instanceof Lt);
        assertEquals(2L, ((Lt) rightRight).getValue());
    }

    @Test
    public void shouldGiveCorrectPredicateStringKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new StringType());
        schema.setRowKeyFields(field);
        List<Field> rowKeyFields = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, "B");
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(field, "A", "Z");
        Region partitionRegion = new Region(partitionRange);
        
        // When
        FilterPredicate predicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(rowKeyFields, Arrays.asList(region), partitionRegion);
        
        // Then
        assertTrue(predicate instanceof And);
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertTrue(left instanceof And);
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertTrue(leftLeft instanceof GtEq);
        assertEquals("A", ((Binary) ((GtEq) leftLeft).getValue()).toStringUsingUTF8());
        assertTrue(leftRight instanceof Lt);
        assertEquals("Z", ((Binary) ((Lt) leftRight).getValue()).toStringUsingUTF8());
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertTrue(right instanceof And);
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertTrue(rightLeft instanceof GtEq);
        assertEquals("B", ((Binary) ((GtEq) rightLeft).getValue()).toStringUsingUTF8());
        assertTrue(rightRight instanceof Lt);
        assertEquals("B" + '\u0000', ((Binary) ((Lt) rightRight).getValue()).toStringUsingUTF8());
    }

    @Test
    public void shouldGiveCorrectPredicateByteArrayKey() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        List<Field> rowKeyFields = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, new byte[]{10, 20});
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(field, new byte[]{1}, new byte[]{50, 61});
        Region partitionRegion = new Region(partitionRange);
        
        // When
        FilterPredicate predicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(rowKeyFields, Arrays.asList(region), partitionRegion);
        
        // Then
        assertTrue(predicate instanceof And);
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertTrue(left instanceof And);
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertTrue(leftLeft instanceof GtEq);
        assertArrayEquals(new byte[]{1}, ((Binary) ((GtEq) leftLeft).getValue()).getBytes());
        assertTrue(leftRight instanceof Lt);
        assertArrayEquals(new byte[]{50, 61}, ((Binary) ((Lt) leftRight).getValue()).getBytes());
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertTrue(right instanceof And);
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertTrue(rightLeft instanceof GtEq);
        assertArrayEquals(new byte[]{10, 20}, ((Binary) ((GtEq) rightLeft).getValue()).getBytes());
        assertTrue(rightRight instanceof Lt);
        assertArrayEquals(new byte[]{10, 20, -128}, ((Binary) ((Lt) rightRight).getValue()).getBytes());
    }
 
    @Test
    public void shouldGiveCorrectPredicateIntKeyMultipleRanges() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        List<Field> rowKeyFields = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createExactRange(field, 1);
        Range range2 = rangeFactory.createRange(field, 5, 7);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        Range partitionRange = rangeFactory.createRange(field, 0, 100);
        Region partitionRegion = new Region(partitionRange);
        
        // When
        FilterPredicate predicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(rowKeyFields, Arrays.asList(region1, region2), partitionRegion);
        
        // Then
        assertTrue(predicate instanceof And);
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertTrue(left instanceof And);
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertTrue(leftLeft instanceof GtEq);
        assertEquals(0, ((GtEq) leftLeft).getValue());
        assertTrue(leftRight instanceof Lt);
        assertEquals(100, ((Lt) leftRight).getValue());
        //  - Right predicate restricts to the ranges
        FilterPredicate right = and.getRight();
        assertTrue(right instanceof Or);
        FilterPredicate rightLeft = ((Or) right).getLeft();
        FilterPredicate rightRight = ((Or) right).getRight();
        assertTrue(rightLeft instanceof And);
        FilterPredicate p1 = ((And) rightLeft).getLeft();
        assertTrue(p1 instanceof GtEq);
        assertEquals(1, ((GtEq) p1).getValue());
        FilterPredicate p2 = ((And) rightLeft).getRight();
        assertTrue(p2 instanceof Lt);
        assertEquals(2, ((Lt) p2).getValue());
        FilterPredicate p3 = ((And) rightRight).getLeft();
        assertTrue(p3 instanceof GtEq);
        assertEquals(5, ((GtEq) p3).getValue());
        FilterPredicate p4 = ((And) rightRight).getRight();
        assertTrue(p4 instanceof Lt);
        assertEquals(7, ((Lt) p4).getValue());
    }

    @Test
    public void shouldGiveCorrectPredicateMultidimKeyMultipleRanges() {
        // Given
        Schema schema = new Schema();
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new IntType());
        schema.setRowKeyFields(field1, field2);
        List<Field> rowKeyFields = schema.getRowKeyFields();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field1, new byte[]{10}, new byte[]{20});
        Range range2 = rangeFactory.createRange(field2, 100, 200);
        Region region = new Region(Arrays.asList(range1, range2));
        Range partitionRange1 = rangeFactory.createRange(field1, new byte[]{10}, new byte[]{40});
        Range partitionRange2 = rangeFactory.createRange(field2, 50, 250);
        Region partitionRegion = new Region(Arrays.asList(partitionRange1, partitionRange2));
        
        // When
        FilterPredicate predicate = RangeQueryUtils.getFilterPredicateMultidimensionalKey(rowKeyFields, Arrays.asList(region), partitionRegion);
        
        // Then
        //  - Predicate should look like the following:
        //        and(
        //                and(
        //                        and(
        //                                gteq(key1, Binary{1 constant bytes, [10]}), lt(key1, Binary{1 constant bytes, [40]})),
        //                        and(
        //                                gteq(key2, 50), lt(key2, 250))),
        //                and(
        //                        and(
        //                                gteq(key1, Binary{1 constant bytes, [10]}), lt(key1, Binary{1 constant bytes, [20]})),
        //                        and(
        //                                gteq(key2, 100), lt(key2, 200))))
        assertTrue(predicate instanceof And);
        And l1And = (And) predicate;
        assertTrue(l1And.getLeft() instanceof And);
        assertTrue(l1And.getRight() instanceof And);
        
        And l2And1 = (And) l1And.getLeft();
        And l2And2 = (And) l1And.getRight();
        assertTrue(l2And1.getLeft() instanceof And);
        assertTrue(l2And1.getRight() instanceof And);
        assertTrue(l2And2.getLeft() instanceof And);
        assertTrue(l2And2.getRight() instanceof And);
        
        And l3And1 = (And) l2And1.getLeft();
        assertTrue(l3And1.getLeft() instanceof GtEq);
        GtEq gteq1 = (GtEq) l3And1.getLeft();
        assertArrayEquals(new byte[]{10}, ((Binary) gteq1.getValue()).getBytes());
        assertTrue(l3And1.getRight() instanceof Lt);
        Lt lt1 = (Lt) l3And1.getRight();
        assertArrayEquals(new byte[]{40}, ((Binary) lt1.getValue()).getBytes());
        
        And l3And2 = (And) l2And1.getRight();
        assertTrue(l3And2.getLeft() instanceof GtEq);
        GtEq gteq2 = (GtEq) l3And2.getLeft();
        assertEquals(50, gteq2.getValue());
        assertTrue(l3And2.getRight() instanceof Lt);
        Lt lt2 = (Lt) l3And2.getRight();
        assertEquals(250, lt2.getValue());
        
        And l3And3 = (And) l2And2.getLeft();
        assertTrue(l3And3.getLeft() instanceof GtEq);
        GtEq gteq3 = (GtEq) l3And3.getLeft();
        assertArrayEquals(new byte[]{10}, ((Binary) gteq3.getValue()).getBytes());
        assertTrue(l3And3.getRight() instanceof Lt);
        Lt lt3 = (Lt) l3And3.getRight();
        assertArrayEquals(new byte[]{20}, ((Binary) lt3.getValue()).getBytes());
        
        And l3And4 = (And) l2And2.getRight();
        assertTrue(l3And4.getLeft() instanceof GtEq);
        GtEq gteq4 = (GtEq) l3And4.getLeft();
        assertEquals(100, gteq4.getValue());
        assertTrue(l3And4.getRight() instanceof Lt);
        Lt lt4 = (Lt) l3And4.getRight();
        assertEquals(200, lt4.getValue());
    }
}
