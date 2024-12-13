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
package sleeper.bulkexport.model;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class BulkExportLeafPartitionQueryTest {

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
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region1 = new Region(rangeFactory.createRange(field, 1L, true, 10L, true));
        String tableId = UUID.randomUUID().toString();
        String leafPartitionId = partitions.getPartition("LL").getId();
        String subExportId = UUID.randomUUID().toString();
        List<String> files = Collections.singletonList("/test/file.parquet");
        BulkExportLeafPartitionQuery query1 = BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId("A")
                .regions(List.of(
                        region1))
                .leafPartitionId(leafPartitionId)
                .subExportId(subExportId)
                .partitionRegion(regionLL)
                .files(files)
                .build();
        BulkExportLeafPartitionQuery query2 = BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId("A")
                .regions(List.of(
                        region1))
                .leafPartitionId(leafPartitionId)
                .subExportId(subExportId)
                .partitionRegion(regionLL)
                .files(files)
                .build();
        BulkExportLeafPartitionQuery query3 = BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId("B")
                .regions(List.of(
                        region1))
                .leafPartitionId(leafPartitionId)
                .subExportId(subExportId)
                .partitionRegion(regionLL)
                .files(files)
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
