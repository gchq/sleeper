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
package sleeper.statestore.transactionlog;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.partition.Partition;
import sleeper.core.range.RegionSerDe;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.ListType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.statestore.StateStoreFileUtils;

import java.util.List;

public class TransactionLogPartitionsSnapshot {
    private final RegionSerDe regionSerDe;
    private final StateStoreFileUtils stateStoreFileUtils;

    TransactionLogPartitionsSnapshot(Schema schema, Configuration configuration) {
        this.regionSerDe = new RegionSerDe(schema);
        this.stateStoreFileUtils = new StateStoreFileUtils(initialisePartitionSchema(), configuration);
    }

    void save(String basePath, StateStorePartitions state, long lastTransactionNumber) throws StateStoreException {
        stateStoreFileUtils.save(createPartitionsPath(basePath, lastTransactionNumber),
                state.all().stream().map(this::getRecordFromPartition));
    }

    StateStorePartitions load(String basePath, long lastTransactionNumber) throws StateStoreException {
        StateStorePartitions partitions = new StateStorePartitions();
        stateStoreFileUtils.load(createPartitionsPath(basePath, lastTransactionNumber))
                .map(this::getPartitionFromRecord)
                .forEach(partitions::put);
        return partitions;
    }

    private Record getRecordFromPartition(Partition partition) {
        Record record = new Record();
        record.put("partitionId", partition.getId());
        record.put("leafPartition", "" + partition.isLeafPartition()); // TODO Change to boolean once boolean is a supported type
        String parentPartitionId;
        if (null == partition.getParentPartitionId()) {
            parentPartitionId = "null";
        } else {
            parentPartitionId = partition.getParentPartitionId();
        }
        record.put("parentPartitionId", parentPartitionId);
        record.put("childPartitionIds", partition.getChildPartitionIds());
        record.put("region", regionSerDe.toJson(partition.getRegion()));
        record.put("dimension", partition.getDimension());
        return record;
    }

    private Partition getPartitionFromRecord(Record record) {
        Partition.Builder partitionBuilder = Partition.builder()
                .id((String) record.get("partitionId"))
                .leafPartition(record.get("leafPartition").equals("true"))
                .childPartitionIds((List<String>) record.get("childPartitionIds"))
                .region(regionSerDe.fromJson((String) record.get("region")))
                .dimension((int) record.get("dimension"));
        String parentPartitionId = (String) record.get("parentPartitionId");
        if (!"null".equals(parentPartitionId)) {
            partitionBuilder.parentPartitionId(parentPartitionId);
        }
        return partitionBuilder.build();
    }

    private String createPartitionsPath(String basePath, long lastTransactionNumber) throws StateStoreException {
        return basePath + "/snapshots/" + lastTransactionNumber + "-partitions.parquet";
    }

    private static Schema initialisePartitionSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("partitionId", new StringType()))
                .valueFields(
                        new Field("leafPartition", new StringType()),
                        new Field("parentPartitionId", new StringType()),
                        new Field("childPartitionIds", new ListType(new StringType())),
                        new Field("region", new StringType()),
                        new Field("dimension", new IntType()))
                .build();
    }

}
