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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import sleeper.core.partition.Partition;
import sleeper.core.range.RegionSerDe;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.StateStorePartitions;
import sleeper.core.statestore.transactionlog.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.TransactionLogStoreLoader;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class S3TransactionLogPartitionsSnapshot implements TransactionLogSnapshot<StateStorePartitions> {
    private static final Schema PARTITION_SCHEMA = TransactionLogSnapshot.initialisePartitionSchema();
    private final RegionSerDe regionSerDe;
    private final TransactionLogStoreLoader<StateStorePartitions> storeLoader;
    private final TransactionLogStore logStore;

    public static S3TransactionLogPartitionsSnapshot fromLogStore(
            Schema schema, TableStatus sleeperTable, TransactionLogStore logStore,
            int maxAddTransactionAttempts, ExponentialBackoffWithJitter backoffWithJitter) {
        return new S3TransactionLogPartitionsSnapshot(schema,
                TransactionLogStoreLoader.forPartitions(sleeperTable, maxAddTransactionAttempts, backoffWithJitter),
                logStore);
    }

    S3TransactionLogPartitionsSnapshot(Schema schema, TransactionLogStoreLoader<StateStorePartitions> storeLoader, TransactionLogStore logStore) {
        this.regionSerDe = new RegionSerDe(schema);
        this.storeLoader = storeLoader;
        this.logStore = logStore;
    }

    public StateStorePartitions state() throws StateStoreException {
        return storeLoader.getState(logStore);
    }

    public long lastTransactionNumber() throws StateStoreException {
        return storeLoader.getLastTransactionNumber(logStore);
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

    public void save(java.nio.file.Path tempDir) throws StateStoreException {
        StateStorePartitions stateStorePartitions = storeLoader.getState(logStore);

        String path = createPath();
        try (ParquetWriter<Record> recordWriter = ParquetRecordWriterFactory.createParquetRecordWriter(
                new Path(path), PARTITION_SCHEMA, new Configuration())) {
            for (Partition partition : stateStorePartitions.all()) {
                recordWriter.write(getRecordFromPartition(partition));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing partitions", e);
        }
    }

    private String createPath() throws StateStoreException {
        return "snapshots/" + storeLoader.getLastTransactionNumber(logStore) + "-partitions.parquet";
    }
}
