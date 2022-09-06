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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import sleeper.core.partition.Partition;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.statestore.dynamodb.DynamoDBAttributes.createNumberAttribute;
import static sleeper.statestore.dynamodb.DynamoDBAttributes.createStringAttribute;

class DynamoDBPartitionFormat {

    static final String ID = "PartitionId";
    static final String IS_LEAF = "PartitionIsLeaf";
    private static final String PARENT_ID = "PartitionParentId";
    private static final String CHILD_IDS = "PartitionChildIds";
    private static final String SPLIT_DIMENSION = "PartitionSplitDimension";
    private static final String REGION = "Region";

    private final List<PrimitiveType> rowKeyTypes;
    private final RegionSerDe regionSerDe;

    DynamoDBPartitionFormat(Schema schema) {
        rowKeyTypes = schema.getRowKeyTypes();
        regionSerDe = new RegionSerDe(schema);
    }

    Map<String, AttributeValue> getItemFromPartition(Partition partition) throws IOException {
        Map<String, AttributeValue> map = new HashMap<>();
        map.put(ID, createStringAttribute(partition.getId()));
        map.put(IS_LEAF, createStringAttribute("" + partition.isLeafPartition()));
        if (null != partition.getParentPartitionId()) {
            map.put(PARENT_ID, createStringAttribute(partition.getParentPartitionId()));
        }
        if (null != partition.getChildPartitionIds() && !partition.getChildPartitionIds().isEmpty()) {
            map.put(CHILD_IDS, createStringAttribute(childPartitionsToString(partition.getChildPartitionIds())));
        }
        map.put(SPLIT_DIMENSION, createNumberAttribute(partition.getDimension()));
        map.put(REGION, createStringAttribute(regionSerDe.toJson(partition.getRegion())));
        return map;
    }

    Partition getPartitionFromAttributeValues(Map<String, AttributeValue> item) throws IOException {
        Partition partition = new Partition();
        partition.setRowKeyTypes(rowKeyTypes);
        partition.setId(item.get(ID).getS());
        partition.setLeafPartition(Boolean.parseBoolean(item.get(IS_LEAF).getS()));
        if (null != item.get(PARENT_ID)) {
            partition.setParentPartitionId(item.get(PARENT_ID).getS());
        }
        if (null != item.get(CHILD_IDS)) {
            String childPartitionIdsString = item.get(CHILD_IDS).getS();
            partition.setChildPartitionIds(childPartitionsFromString(childPartitionIdsString));
        }
        if (null != item.get(SPLIT_DIMENSION)) {
            partition.setDimension(Integer.parseInt(item.get(SPLIT_DIMENSION).getN()));
        }

        partition.setRegion(regionSerDe.fromJson(item.get(REGION).getS()));

        return partition;
    }

    private static String childPartitionsToString(List<String> childPartitionIds) {
        if (null == childPartitionIds || childPartitionIds.isEmpty()) {
            return null;
        }
        return String.join("___", childPartitionIds);
    }

    private static List<String> childPartitionsFromString(String childPartitionsString) {
        if (null == childPartitionsString) {
            return new ArrayList<>();
        }
        String[] childPartitions = childPartitionsString.split("___");
        return Arrays.asList(childPartitions);
    }

}
