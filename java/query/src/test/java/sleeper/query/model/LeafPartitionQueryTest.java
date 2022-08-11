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
package sleeper.query.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;

public class LeafPartitionQueryTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, 1L, true, 10L, true);
        Range range2 = rangeFactory.createRange(field, 1L, true, 11L, true);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        Range partitionRange1 = rangeFactory.createRange(field, 0L, 200L);
        Range partitionRange2 = rangeFactory.createRange(field, 10L, 200L);
        Range partitionRange3 = rangeFactory.createRange(field, 0L, 123456L);
        Region partitionRegion1 = new Region(partitionRange1);
        Region partitionRegion2 = new Region(partitionRange2);
        Region partitionRegion3 = new Region(partitionRange3);
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        List<String> files2 = new ArrayList<>();
        files2.add("file3");
        LeafPartitionQuery query1 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region1, "leaf",
                partitionRegion1,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query2 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region1, "leaf",
                partitionRegion1,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query3 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region2, "leaf",
                partitionRegion2,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query4 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region2, "leaf",
                partitionRegion3,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query5 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery2", region2, "leaf",
                partitionRegion3,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query6 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery2", region2, "leaf2",
                partitionRegion3,
                files2)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        boolean test3 = query1.equals(query4);
        boolean test4 = query1.equals(query5);
        boolean test5 = query1.equals(query6);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();
        int hashCode4 = query4.hashCode();
        int hashCode5 = query5.hashCode();
        int hashCode6 = query6.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(test3).isFalse();
        assertThat(test4).isFalse();
        assertThat(test5).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
        assertThat(hashCode4).isNotEqualTo(hashCode1);
        assertThat(hashCode5).isNotEqualTo(hashCode1);
        assertThat(hashCode6).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWhenOnlyRangeIsDifferent() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, 1L, true, 10L, true);
        Range range2 = rangeFactory.createRange(field, 1L, true, 11L, true);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        Range partitionRange = rangeFactory.createRange(field, 0L, 200L);
        Region partitionRegion = new Region(partitionRange);
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        LeafPartitionQuery query1 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region1, "leaf",
                partitionRegion,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query2 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region1, "leaf",
                partitionRegion,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query3 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region2, "leaf",
                partitionRegion,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }

    @Test
    public void testEqualsAndHashcodeWithByteArray() {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range1 = rangeFactory.createRange(field, new byte[]{1}, true, new byte[]{10}, true);
        Range range2 = rangeFactory.createRange(field, new byte[]{20}, true, new byte[]{30}, true);
        Region region1 = new Region(range1);
        Region region2 = new Region(range2);
        Range partitionRange1 = rangeFactory.createRange(field, new byte[]{0}, new byte[]{100});
        Range partitionRange2 = rangeFactory.createRange(field, new byte[]{1}, new byte[]{100});
        Region partitionRegion1 = new Region(partitionRange1);
        Region partitionRegion2 = new Region(partitionRange2);
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        LeafPartitionQuery query1 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region1, "leaf",
                partitionRegion1,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query2 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region1, "leaf",
                partitionRegion1,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();
        LeafPartitionQuery query3 = new LeafPartitionQuery.Builder(
                "myTable", "id", "subQuery", region2, "leaf",
                partitionRegion2,
                files)
                .setQueryTimeIteratorClassName("iteratorClassName")
                .setQueryTimeIteratorConfig("iteratorConfig")
                .setResultsPublisherConfig(new HashMap<>())
                .setRequestedValueFields(new ArrayList<>()).build();

        // When
        boolean test1 = query1.equals(query2);
        boolean test2 = query1.equals(query3);
        int hashCode1 = query1.hashCode();
        int hashCode2 = query2.hashCode();
        int hashCode3 = query3.hashCode();

        // Then
        assertThat(test1).isTrue();
        assertThat(test2).isFalse();
        assertThat(hashCode2).isEqualTo(hashCode1);
        assertThat(hashCode3).isNotEqualTo(hashCode1);
    }
}
