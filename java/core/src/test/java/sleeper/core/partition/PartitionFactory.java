/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.core.partition;

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A convenience class for specifying partitions.
 * <p>
 * Note that a shorthand is used for cases where we have a schema with only one row key field.
 * This will not be useful in the general case.
 */
public class PartitionFactory {

    private final Schema schema;
    private final Range.RangeFactory rangeFactory;
    private final List<PrimitiveType> rowKeyTypes;

    public PartitionFactory(Schema schema) {
        this.schema = schema;
        rangeFactory = new Range.RangeFactory(schema);
        rowKeyTypes = schema.getRowKeyTypes();
    }

    public Partition.Builder partition(String id, Object min, Object max) {
        return partition(id, new Region(rangeFactory.createRange(singleRowKeyField(), min, max)));
    }

    public Partition.Builder partition(String id, Region region) {
        return Partition.builder()
                .rowKeyTypes(rowKeyTypes)
                .region(region)
                .id(id)
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1);
    }

    public List<Partition.Builder> split(Partition parent, String leftId, String rightId, int dimension, Object splitPoint) {
        Field splitField = schema.getRowKeyFields().get(dimension);
        Region parentRegion = parent.getRegion();
        Range parentRange = parentRegion.getRange(splitField.getName());
        Range leftRange = rangeFactory.createRange(splitField, parentRange.getMin(), splitPoint);
        Range rightRange = rangeFactory.createRange(splitField, splitPoint, parentRange.getMax());
        Partition.Builder leftPartition = partition(leftId, parentRegion.childWithRange(leftRange));
        Partition.Builder rightPartition = partition(rightId, parentRegion.childWithRange(rightRange));
        leftPartition.parentPartitionId(parent.getId());
        rightPartition.parentPartitionId(parent.getId());
        parent = parent.toBuilder()
                .leafPartition(false)
                .dimension(dimension)
                .childPartitionIds(List.of(leftId, rightId))
                .build();
        return List.of(leftPartition, rightPartition);
    }

    public Partition.Builder parentJoining(String parentId, Partition.Builder left, Partition.Builder right) {
        return parent(Arrays.asList(left, right), parentId, parentRegion(left.getRegion(), right.getRegion()));
    }

    public Partition.Builder rootFirst(String id) {
        return PartitionsFromSplitPoints
                .createRootPartitionThatIsLeaf(schema, rangeFactory)
                .id(id);
    }

    private Partition.Builder parent(List<Partition.Builder> children, String id, Region region) {
        Partition.Builder parent = partition(id, region)
                .childPartitionIds(children.stream()
                        .map(Partition.Builder::getId)
                        .collect(Collectors.toList()))
                .leafPartition(false)
                .dimension(0);
        children.forEach(child -> child.parentPartitionId(id));
        return parent;
    }

    private Region parentRegion(Region left, Region right) {
        return new Region(schema.getRowKeyFields().stream()
                .map(field -> rangeFactory.createRange(field,
                        left.getRange(field.getName()).getMin(),
                        right.getRange(field.getName()).getMax()))
                .collect(Collectors.toList()));
    }

    private Field singleRowKeyField() {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        if (rowKeyFields.size() != 1) {
            throw new IllegalStateException("Cannot get single row key field, have " + rowKeyFields.size() + " row key fields");
        }
        return rowKeyFields.get(0);
    }
}
