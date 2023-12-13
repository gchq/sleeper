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
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfoFactory;

import java.util.List;

import static org.approvaltests.Approvals.verify;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TableFileInfoPrinterTest {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema);

    @Test
    void shouldPrintFilesOnLeaves() {
        partitions.rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .splitToNewChildren("L", "LL", "LR", "row-25")
                .splitToNewChildren("R", "RL", "RR", "row-75")
                .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                .splitToNewChildren("RR", "RRL", "RRR", "row-87");

        FileInfoFactory fileInfoFactory = fileInfoFactory();
        verify(TableFileInfoPrinter.printFiles(partitions.buildTree(), List.of(
                fileInfoFactory.partitionFile("LLL", 12),
                fileInfoFactory.partitionFile("LLR", 13),
                fileInfoFactory.partitionFile("LRL", 12),
                fileInfoFactory.partitionFile("LRR", 13),
                fileInfoFactory.partitionFile("RLL", 12),
                fileInfoFactory.partitionFile("RLR", 13),
                fileInfoFactory.partitionFile("RRL", 12),
                fileInfoFactory.partitionFile("RRR", 13))));
    }

    @Test
    void shouldRenamePartitionsByLocation() {
        partitions.rootFirst("base")
                .splitToNewChildren("base", "l", "r", "row-50")
                .splitToNewChildren("l", "ll", "lr", "row-25")
                .splitToNewChildren("r", "rl", "rr", "row-75")
                .splitToNewChildren("ll", "1", "2", "row-12")
                .splitToNewChildren("lr", "3", "4", "row-37")
                .splitToNewChildren("rl", "5", "6", "row-62")
                .splitToNewChildren("rr", "7", "8", "row-87");

        FileInfoFactory fileInfoFactory = fileInfoFactory();
        verify(TableFileInfoPrinter.printFiles(partitions.buildTree(), List.of(
                fileInfoFactory.partitionFile("1", 12),
                fileInfoFactory.partitionFile("2", 13),
                fileInfoFactory.partitionFile("3", 12),
                fileInfoFactory.partitionFile("4", 13),
                fileInfoFactory.partitionFile("5", 12),
                fileInfoFactory.partitionFile("6", 13),
                fileInfoFactory.partitionFile("7", 12),
                fileInfoFactory.partitionFile("8", 13))));
    }

    private FileInfoFactory fileInfoFactory() {
        return FileInfoFactory.from(partitions.buildTree());
    }
}
