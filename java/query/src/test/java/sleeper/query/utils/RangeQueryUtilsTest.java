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

import static org.assertj.core.api.Assertions.assertThat;
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
        assertThat(predicate instanceof And).isTrue();
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertThat(left instanceof And).isTrue();
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertThat(leftLeft instanceof GtEq).isTrue();
        assertThat(((GtEq) leftLeft).getValue()).isEqualTo(0);
        assertThat(leftRight instanceof Lt).isTrue();
        assertThat(((Lt) leftRight).getValue()).isEqualTo(100);
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertThat(right instanceof And).isTrue();
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertThat(rightLeft instanceof GtEq).isTrue();
        assertThat(((GtEq) rightLeft).getValue()).isEqualTo(1);
        assertThat(rightRight instanceof Lt).isTrue();
        assertThat(((Lt) rightRight).getValue()).isEqualTo(2);
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
        assertThat(predicate instanceof And).isTrue();
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertThat(left instanceof And).isTrue();
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertThat(leftLeft instanceof GtEq).isTrue();
        assertThat(((GtEq) leftLeft).getValue()).isEqualTo(0L);
        assertThat(leftRight instanceof Lt).isTrue();
        assertThat(((Lt) leftRight).getValue()).isEqualTo(100L);
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertThat(right instanceof And).isTrue();
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertThat(rightLeft instanceof GtEq).isTrue();
        assertThat(((GtEq) rightLeft).getValue()).isEqualTo(1L);
        assertThat(rightRight instanceof Lt).isTrue();
        assertThat(((Lt) rightRight).getValue()).isEqualTo(2L);
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
        assertThat(predicate instanceof And).isTrue();
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertThat(left instanceof And).isTrue();
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertThat(leftLeft instanceof GtEq).isTrue();
        assertThat(((Binary) ((GtEq) leftLeft).getValue()).toStringUsingUTF8()).isEqualTo("A");
        assertThat(leftRight instanceof Lt).isTrue();
        assertThat(((Binary) ((Lt) leftRight).getValue()).toStringUsingUTF8()).isEqualTo("Z");
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertThat(right instanceof And).isTrue();
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertThat(rightLeft instanceof GtEq).isTrue();
        assertThat(((Binary) ((GtEq) rightLeft).getValue()).toStringUsingUTF8()).isEqualTo("B");
        assertThat(rightRight instanceof Lt).isTrue();
        assertThat(((Binary) ((Lt) rightRight).getValue()).toStringUsingUTF8()).isEqualTo("B" + '\u0000');
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
        assertThat(predicate instanceof And).isTrue();
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertThat(left instanceof And).isTrue();
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertThat(leftLeft instanceof GtEq).isTrue();
        assertThat(((Binary) ((GtEq) leftLeft).getValue()).getBytes()).containsExactly(new byte[]{1});
        assertThat(leftRight instanceof Lt).isTrue();
        assertThat(((Binary) ((Lt) leftRight).getValue()).getBytes()).containsExactly(new byte[]{50, 61});
        //  - Right predicate restricts to the range
        FilterPredicate right = and.getRight();
        assertThat(right instanceof And).isTrue();
        FilterPredicate rightLeft = ((And) right).getLeft();
        FilterPredicate rightRight = ((And) right).getRight();
        assertThat(rightLeft instanceof GtEq).isTrue();
        assertThat(((Binary) ((GtEq) rightLeft).getValue()).getBytes()).containsExactly(new byte[]{10, 20});
        assertThat(rightRight instanceof Lt).isTrue();
        assertThat(((Binary) ((Lt) rightRight).getValue()).getBytes()).containsExactly(new byte[]{10, 20, -128});
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
        assertThat(predicate instanceof And).isTrue();
        And and = (And) predicate;
        //  - Left predicate restricts to the partition
        FilterPredicate left = and.getLeft();
        assertThat(left instanceof And).isTrue();
        FilterPredicate leftLeft = ((And) left).getLeft();
        FilterPredicate leftRight = ((And) left).getRight();
        assertThat(leftLeft instanceof GtEq).isTrue();
        assertThat(((GtEq) leftLeft).getValue()).isEqualTo(0);
        assertThat(leftRight instanceof Lt).isTrue();
        assertThat(((Lt) leftRight).getValue()).isEqualTo(100);
        //  - Right predicate restricts to the ranges
        FilterPredicate right = and.getRight();
        assertThat(right instanceof Or).isTrue();
        FilterPredicate rightLeft = ((Or) right).getLeft();
        FilterPredicate rightRight = ((Or) right).getRight();
        assertThat(rightLeft instanceof And).isTrue();
        FilterPredicate p1 = ((And) rightLeft).getLeft();
        assertThat(p1 instanceof GtEq).isTrue();
        assertThat(((GtEq) p1).getValue()).isEqualTo(1);
        FilterPredicate p2 = ((And) rightLeft).getRight();
        assertThat(p2 instanceof Lt).isTrue();
        assertThat(((Lt) p2).getValue()).isEqualTo(2);
        FilterPredicate p3 = ((And) rightRight).getLeft();
        assertThat(p3 instanceof GtEq).isTrue();
        assertThat(((GtEq) p3).getValue()).isEqualTo(5);
        FilterPredicate p4 = ((And) rightRight).getRight();
        assertThat(p4 instanceof Lt).isTrue();
        assertThat(((Lt) p4).getValue()).isEqualTo(7);
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
        assertThat(predicate instanceof And).isTrue();
        And l1And = (And) predicate;
        assertThat(l1And.getLeft() instanceof And).isTrue();
        assertThat(l1And.getRight() instanceof And).isTrue();

        And l2And1 = (And) l1And.getLeft();
        And l2And2 = (And) l1And.getRight();
        assertThat(l2And1.getLeft() instanceof And).isTrue();
        assertThat(l2And1.getRight() instanceof And).isTrue();
        assertThat(l2And2.getLeft() instanceof And).isTrue();
        assertThat(l2And2.getRight() instanceof And).isTrue();

        And l3And1 = (And) l2And1.getLeft();
        assertThat(l3And1.getLeft() instanceof GtEq).isTrue();
        GtEq gteq1 = (GtEq) l3And1.getLeft();
        assertThat(((Binary) gteq1.getValue()).getBytes()).containsExactly(new byte[]{10});
        assertThat(l3And1.getRight() instanceof Lt).isTrue();
        Lt lt1 = (Lt) l3And1.getRight();
        assertThat(((Binary) lt1.getValue()).getBytes()).containsExactly(new byte[]{40});

        And l3And2 = (And) l2And1.getRight();
        assertThat(l3And2.getLeft() instanceof GtEq).isTrue();
        GtEq gteq2 = (GtEq) l3And2.getLeft();
        assertThat(gteq2.getValue()).isEqualTo(50);
        assertThat(l3And2.getRight() instanceof Lt).isTrue();
        Lt lt2 = (Lt) l3And2.getRight();
        assertThat(lt2.getValue()).isEqualTo(250);

        And l3And3 = (And) l2And2.getLeft();
        assertThat(l3And3.getLeft() instanceof GtEq).isTrue();
        GtEq gteq3 = (GtEq) l3And3.getLeft();
        assertThat(((Binary) gteq3.getValue()).getBytes()).containsExactly(new byte[]{10});
        assertThat(l3And3.getRight() instanceof Lt).isTrue();
        Lt lt3 = (Lt) l3And3.getRight();
        assertThat(((Binary) lt3.getValue()).getBytes()).containsExactly(new byte[]{20});

        And l3And4 = (And) l2And2.getRight();
        assertThat(l3And4.getLeft() instanceof GtEq).isTrue();
        GtEq gteq4 = (GtEq) l3And4.getLeft();
        assertThat(gteq4.getValue()).isEqualTo(100);
        assertThat(l3And4.getRight() instanceof Lt).isTrue();
        Lt lt4 = (Lt) l3And4.getRight();
        assertThat(lt4.getValue()).isEqualTo(200);
    }
}
