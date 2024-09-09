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
package sleeper.query.model;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class LeafPartitionQueryTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionTree partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 100L)
                .splitToNewChildren("L", "LL", "LR", 50L)
                .buildTree();
        Region regionLL = partitions.getPartition("LL").getRegion();
        Region regionLR = partitions.getPartition("LR").getRegion();
        Region regionR = partitions.getPartition("R").getRegion();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        Region region2 = new Region(rangeFactory.createRange(field, 75L, true, 110L, true));
        List<String> files = new ArrayList<>();
        files.add("file1");
        files.add("file2");
        List<String> files2 = new ArrayList<>();
        files2.add("file3");
        LeafPartitionQuery query1 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region1))
                .leafPartitionId("leaf").partitionRegion(regionLL).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query2 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region1))
                .leafPartitionId("leaf").partitionRegion(regionLL).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query3 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region2))
                .leafPartitionId("leaf").partitionRegion(regionLR).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query4 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region2))
                .leafPartitionId("leaf").partitionRegion(regionR).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query5 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery2").regions(List.of(region2))
                .leafPartitionId("leaf").partitionRegion(regionR).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query6 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery2").regions(List.of(region2))
                .leafPartitionId("leaf2").partitionRegion(regionR).files(files2)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();

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
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
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
        LeafPartitionQuery query1 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region1))
                .leafPartitionId("leaf").partitionRegion(partitionRegion).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query2 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region1))
                .leafPartitionId("leaf").partitionRegion(partitionRegion).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query3 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region2))
                .leafPartitionId("leaf").partitionRegion(partitionRegion).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();

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
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
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
        LeafPartitionQuery query1 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region1))
                .leafPartitionId("leaf").partitionRegion(partitionRegion1).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query2 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region1))
                .leafPartitionId("leaf").partitionRegion(partitionRegion1).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();
        LeafPartitionQuery query3 = LeafPartitionQuery.builder()
                .tableId("myTable").queryId("id").subQueryId("subQuery").regions(List.of(region2))
                .leafPartitionId("leaf").partitionRegion(partitionRegion2).files(files)
                .processingConfig(QueryProcessingConfig.builder()
                        .queryTimeIteratorClassName("iteratorClassName")
                        .queryTimeIteratorConfig("iteratorConfig")
                        .build())
                .build();

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
