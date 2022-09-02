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

public class PartitionFactory {

    private final Schema schema;
    private final Range.RangeFactory rangeFactory;
    private final List<PrimitiveType> rowKeyTypes;

    public PartitionFactory(Schema schema) {
        this.schema = schema;
        rangeFactory = new Range.RangeFactory(schema);
        rowKeyTypes = schema.getRowKeyTypes();
    }

    public Partition join(String parentId, Partition left, Partition right) {
        return parent(Arrays.asList(left, right), parentId, singleRange(left).getMin(), singleRange(right).getMax());
    }

    public Partition child(Partition parent, String id, Object min, Object max) {
        Partition child = partition(id, min, max);
        child.setParentPartitionId(parent.getId());
        parent.getChildPartitionIds().add(id);
        parent.setLeafPartition(false);
        parent.setDimension(0);
        return child;
    }

    public Partition parent(List<Partition> children, String id, Object min, Object max) {
        Partition parent = partition(id, min, max);
        parent.setChildPartitionIds(children.stream()
                .map(Partition::getId)
                .collect(Collectors.toList()));
        parent.setLeafPartition(false);
        parent.setDimension(0);
        children.forEach(child -> child.setParentPartitionId(id));
        return parent;
    }

    public Partition partition(String id, Object min, Object max) {
        return partition(id, new Region(rangeFactory.createRange(singleRowKeyField(), min, max)));
    }

    public Partition partition(String id, Region region) {
        return new Partition(rowKeyTypes, region,
                id, true, null, Collections.emptyList(), -1);
    }

    public Range singleRange(Partition partition) {
        return partition.getRegion().getRange(singleRowKeyField().getName());
    }

    private Field singleRowKeyField() {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        if (rowKeyFields.size() != 1) {
            throw new IllegalStateException("Cannot get single row key field, have " + rowKeyFields.size() + " row key fields");
        }
        return rowKeyFields.get(0);
    }
}
