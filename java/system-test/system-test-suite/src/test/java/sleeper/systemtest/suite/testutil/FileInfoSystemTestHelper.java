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

package sleeper.systemtest.suite.testutil;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FileInfoSystemTestHelper {
    private final Schema schema;
    private final PartitionTree tree;
    private final FileInfoFactory fileInfoFactory;

    private FileInfoSystemTestHelper(SleeperSystemTest sleeper) {
        this.schema = sleeper.tableProperties().getSchema();
        this.tree = new PartitionTree(schema, sleeper.partitioning().allPartitions());
        this.fileInfoFactory = FileInfoFactory.from(tree);
    }

    public static FileInfoSystemTestHelper fileInfoHelper(SleeperSystemTest sleeper) {
        return new FileInfoSystemTestHelper(sleeper);
    }

    public static FileInfoFactory fileInfoFactory(
            Schema schema, String tableId, Map<String, List<Partition>> allPartitionsByTable) {
        return FileInfoFactory.from(schema, allPartitionsByTable.get(tableId));
    }

    public static long numberOfRecordsIn(List<? extends FileInfo> files) {
        return files.stream().mapToLong(FileInfo::getNumberOfRecords).sum();
    }

    public FileInfo leafFile(long records, Object min, Object max) {
        return fileInfoFactory.partitionFile(getPartitionId(min, max), records);
    }

    public String getPartitionId(Object min, Object max) {
        if (min == null && max == null) {
            Partition partition = tree.getRootPartition();
            if (!partition.getChildPartitionIds().isEmpty()) {
                throw new IllegalArgumentException("Cannot choose leaf partition, root partition is not a leaf partition");
            }
            return partition.getId();
        }
        Partition partition = tree.getLeafPartition(Objects.requireNonNull(rowKey(min)));
        if (!partition.isRowKeyInPartition(schema, rowKey(max))) {
            throw new IllegalArgumentException("Not in same leaf partition: " + min + ", " + max);
        }
        return partition.getId();
    }

    private static Key rowKey(Object value) {
        if (value == null) {
            return null;
        } else {
            return Key.create(value);
        }
    }
}
