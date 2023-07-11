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

    public Partition partition(String id, Object min, Object max) {
        return partition(id, new Region(rangeFactory.createRange(singleRowKeyField(), min, max))).build();
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

    public List<Partition> split(Partition parent, String leftId, String rightId, int dimension, Object splitPoint) {
        Field splitField = schema.getRowKeyFields().get(dimension);
        Region parentRegion = parent.getRegion();
        Range parentRange = parentRegion.getRange(splitField.getName());
        Range leftRange = rangeFactory.createRange(splitField, parentRange.getMin(), splitPoint);
        Range rightRange = rangeFactory.createRange(splitField, splitPoint, parentRange.getMax());
        Partition.Builder leftPartition = partition(leftId, parentRegion.childWithRange(leftRange));
        Partition.Builder rightPartition = partition(rightId, parentRegion.childWithRange(rightRange));
        leftPartition.id(parent.getId());
        rightPartition.id(parent.getId());
        parent.setChildPartitionIds(List.of(leftId, rightId));
        parent.setLeafPartition(false);
        parent.setDimension(dimension);
        return List.of(leftPartition.build(), rightPartition.build());
    }

    public Partition parentJoining(String parentId, Partition left, Partition right) {
        return parent(Arrays.asList(left, right), parentId, parentRegion(left.getRegion(), right.getRegion()));
    }

    public Partition rootFirst(String id) {
        return PartitionsFromSplitPoints
                .createRootPartitionThatIsLeaf(schema, rangeFactory)
                .id(id)
                .build();
    }

    private Partition parent(List<Partition> children, String id, Region region) {
        Partition parent = partition(id, region).build();
        parent.setChildPartitionIds(children.stream()
                .map(Partition::getId)
                .collect(Collectors.toList()));
        parent.setLeafPartition(false);
        parent.setDimension(0);
        children.forEach(child -> child.setParentPartitionId(id));
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
