/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.systemtest.dsl.testutil;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionTreeTestHelper;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SleeperDsl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class SystemTestPartitionsTestHelper {

    private SystemTestPartitionsTestHelper() {
    }

    public static PartitionTree create2StringPartitions(SleeperDsl sleeper) {
        return createStringPartitionsFromSplitPointsDirectory(sleeper, "2-partitions.txt");
    }

    public static PartitionTree create128StringPartitions(SleeperDsl sleeper) {
        return createStringPartitionsFromSplitPointsDirectory(sleeper, "128-partitions.txt");
    }

    public static PartitionTree create512StringPartitions(SleeperDsl sleeper) {
        return createStringPartitionsFromSplitPointsDirectory(sleeper, "512-partitions.txt");
    }

    public static PartitionTree create8192StringPartitions(SleeperDsl sleeper) {
        return createStringPartitionsFromSplitPointsDirectory(sleeper, "8192-partitions.txt");
    }

    public static PartitionTree createStringPartitionsFromSplitPointsDirectory(
            SleeperDsl sleeper, String filename) {
        return createPartitionsFromSplitPoints(sleeper.tableProperties().getSchema(),
                readStringSplitPoints(sleeper.getSplitPointsDirectory()
                        .resolve("string").resolve(filename)));
    }

    private static PartitionTree createPartitionsFromSplitPoints(Schema schema, List<Object> splitPoints) {
        return new PartitionTree(new PartitionsFromSplitPoints(schema, splitPoints).construct());
    }

    public static PartitionTree createPartitionTreeWithRowsPerPartitionAndTotal(int rowsPerPartition, int totalRows, SleeperDsl sleeper) {
        return PartitionTreeTestHelper.createPartitionTreeWithRowsPerPartitionAndTotal(
                rowsPerPartition, totalRows,
                sleeper.generateNumberedRows()::row,
                sleeper.tableProperties().getSchema());
    }

    private static List<Object> readStringSplitPoints(Path file) {
        try {
            return Collections.unmodifiableList(Files.readAllLines(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static PartitionsBuilder partitionsBuilder(SleeperDsl sleeper) {
        return new PartitionsBuilder(sleeper.tableProperties().getSchema());
    }

    public static PartitionsBuilder partitionsBuilder(Schema schema) {
        return new PartitionsBuilder(schema);
    }

    public static PartitionTree singleRootPartition(Schema schema) {
        return partitionsBuilder(schema).singlePartition("root").buildTree();
    }
}
