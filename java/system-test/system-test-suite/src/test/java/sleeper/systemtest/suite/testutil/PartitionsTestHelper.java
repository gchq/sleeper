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

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class PartitionsTestHelper {

    private PartitionsTestHelper() {
    }

    public static PartitionTree create128StringPartitions(SleeperSystemTest sleeper) {
        return createStringPartitionsFromSplitPointsDirectory(sleeper, "128-partitions.txt");
    }

    public static PartitionTree create512StringPartitions(SleeperSystemTest sleeper) {
        return createStringPartitionsFromSplitPointsDirectory(sleeper, "512-partitions.txt");
    }

    public static PartitionTree createStringPartitionsFromSplitPointsDirectory(
            SleeperSystemTest sleeper, String filename) {
        return createPartitionsFromSplitPoints(sleeper.tableProperties().getSchema(),
                readStringSplitPoints(sleeper.getSplitPointsDirectory()
                        .resolve("string").resolve(filename)));
    }

    private static PartitionTree createPartitionsFromSplitPoints(Schema schema, List<Object> splitPoints) {
        return new PartitionTree(schema,
                new PartitionsFromSplitPoints(schema, splitPoints).construct());
    }

    private static List<Object> readStringSplitPoints(Path file) {
        try {
            return Collections.unmodifiableList(Files.readAllLines(file));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
