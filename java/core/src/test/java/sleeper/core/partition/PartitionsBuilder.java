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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A convenience class for specifying partitions.
 * <p>
 * Note that a shorthand is used for cases where we have a schema with only one row key field.
 * This will not be useful in the general case.
 */
public class PartitionsBuilder {

    private final Schema schema;
    private final List<PrimitiveType> rowKeyTypes;
    private final List<Partition> partitions = new ArrayList<>();

    public PartitionsBuilder(Schema schema) {
        this.schema = schema;
        rowKeyTypes = schema.getRowKeyTypes();
    }

    public Partition partition(String id, Object min, Object max) {
        Partition partition = new Partition(rowKeyTypes, new Region(new Range(singleRowKeyField(), min, max)),
                id, true, null, Collections.emptyList(), -1);
        partitions.add(partition);
        return partition;
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

    public List<Partition> getPartitions() {
        return partitions;
    }

    public PartitionTree getPartitionTree() {
        return new PartitionTree(schema, partitions);
    }

    private Field singleRowKeyField() {
        List<Field> rowKeyFields = schema.getRowKeyFields();
        if (rowKeyFields.size() != 1) {
            throw new IllegalStateException("Cannot get single row key field, have " + rowKeyFields.size());
        }
        return rowKeyFields.get(0);
    }
}
