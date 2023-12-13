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

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.List;
import java.util.Map;

import static org.approvaltests.Approvals.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TablePartitionsPrinterTest {

    private final Schema schema = schemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema);

    @Test
    void shouldPrintTreeWith8LevelsOfSplits() {
        partitions.rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .splitToNewChildren("L", "LL", "LR", 25L)
                .splitToNewChildren("R", "RL", "RR", 75L)
                .splitToNewChildren("LL", "LLL", "LLR", 12L)
                .splitToNewChildren("LR", "LRL", "LRR", 37L)
                .splitToNewChildren("RL", "RLL", "RLR", 62L)
                .splitToNewChildren("RR", "RRL", "RRR", 87L);

        verify(TablePartitionsPrinter.printPartitions(schema, partitions.buildTree()));
    }

    @Test
    void shouldRenamePartitionsByLocation() {
        partitions.rootFirst("base")
                .splitToNewChildren("base", "l", "r", 50L)
                .splitToNewChildren("l", "ll", "lr", 25L)
                .splitToNewChildren("r", "rl", "rr", 75L)
                .splitToNewChildren("ll", "1", "2", 12L)
                .splitToNewChildren("lr", "3", "4", 37L)
                .splitToNewChildren("rl", "5", "6", 62L)
                .splitToNewChildren("rr", "7", "8", 87L);

        verify(TablePartitionsPrinter.printPartitions(schema, partitions.buildTree()));
    }

    @Test
    void shouldPrintDifferentPartitionsForOneTable() {
        PartitionsBuilder partitions1 = new PartitionsBuilder(schema).rootFirst("A")
                .splitToNewChildren("A", "B", "C", 10L);
        PartitionsBuilder partitions2 = new PartitionsBuilder(schema).rootFirst("1")
                .splitToNewChildren("1", "2", "3", 20L);

        verify(TablePartitionsPrinter.printTablePartitionsExpectingIdentical(schema, Map.of(
                "table-1", partitions1.buildTree(),
                "table-2", partitions2.buildTree(),
                "table-3", partitions1.buildTree())));
    }

    @Test
    void shouldPrintExpectedForTables() {
        partitions.rootFirst("A")
                .splitToNewChildren("A", "B", "C", 10L);

        assertThat(TablePartitionsPrinter.printExpectedPartitionsForAllTables(schema, List.of("table-1", "table-2"), partitions.buildTree()))
                .isEqualTo(TablePartitionsPrinter.printTablePartitionsExpectingIdentical(schema, Map.of(
                        "table-1", partitions.buildTree(), "table-2", partitions.buildTree())));
    }
}
