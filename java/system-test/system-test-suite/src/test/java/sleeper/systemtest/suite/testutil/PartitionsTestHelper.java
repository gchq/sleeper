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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartitionsTestHelper {

    private PartitionsTestHelper() {
    }

    public static PartitionTree create128Partitions(SleeperSystemTest sleeper) {
        return create128Partitions(sleeper.tableProperties().getSchema());
    }

    public static PartitionTree create512Partitions(SleeperSystemTest sleeper) {
        return createPartitionsFromSplitPoints(sleeper.tableProperties().getSchema(), create511SplitPoints());
    }

    static PartitionTree create128Partitions(Schema schema) {
        return createPartitionsFromSplitPoints(schema, create127SplitPoints());
    }

    static List<Object> create127SplitPoints() {
        return IntStream.range(1, 128)
                .mapToObj(i -> "" + (char) (i / 5 + 'a') + SECONDARY_SPLITS_FOR_127.get(i % 5))
                .collect(Collectors.toUnmodifiableList());
    }

    static List<Object> create511SplitPoints() {
        return IntStream.range(1, 512)
                .mapToObj(i -> "" + (char) (i / 20 + 'a') + SECONDARY_SPLITS_FOR_511.get(i % 20))
                .collect(Collectors.toUnmodifiableList());
    }

    private static final List<Character> SECONDARY_SPLITS_FOR_127 = secondarySplitCharacters(5);
    private static final List<Character> SECONDARY_SPLITS_FOR_511 = secondarySplitCharacters(20);

    private static List<Character> secondarySplitCharacters(int n) {
        double step = 26.0 / (double) n;
        return IntStream.range(0, n)
                .map(i -> (int) (i * step))
                .mapToObj(PartitionsTestHelper::charN)
                .collect(Collectors.toUnmodifiableList());
    }

    private static char charN(int n) {
        return (char) (n + 'a');
    }

    private static PartitionTree createPartitionsFromSplitPoints(Schema schema, List<Object> splitPoints) {
        return new PartitionTree(schema,
                new PartitionsFromSplitPoints(schema, splitPoints).construct());
    }
}
