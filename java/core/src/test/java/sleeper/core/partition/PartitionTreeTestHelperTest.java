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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.testutils.printers.PartitionsPrinter;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.partition.PartitionTreeTestHelper.createPartitionTreeWithRowsPerPartitionAndTotal;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class PartitionTreeTestHelperTest {

    @Test
    void shouldCreateSplitPointsFromRowRangeAndRowsPerPartition() {
        // Given
        Schema schema = createSchemaWithKey("key", new IntType());
        List<Row> rows = List.of(
                new Row(Map.of("key", 10)),
                new Row(Map.of("key", 20)),
                new Row(Map.of("key", 30)));

        // When
        PartitionTree tree = createPartitionTreeWithRowsPerPartition(1, rows, schema);

        // Then
        assertThat(PartitionsPrinter.printPartitions(schema, tree))
                .isEqualTo("""
                        Leaf partition at LL:
                        {"key":{"min":-2147483648,"minInclusive":true,"max":20,"maxInclusive":false},"stringsBase64Encoded":true}
                        Leaf partition at LR:
                        {"key":{"min":20,"minInclusive":true,"max":30,"maxInclusive":false},"stringsBase64Encoded":true}
                        Leaf partition at R:
                        {"key":{"min":30,"minInclusive":true,"max":null,"maxInclusive":false},"stringsBase64Encoded":true}
                        Partition at L:
                        {"key":{"min":-2147483648,"minInclusive":true,"max":30,"maxInclusive":false},"stringsBase64Encoded":true}
                        Partition at root:
                        {"key":{"min":-2147483648,"minInclusive":true,"max":null,"maxInclusive":false},"stringsBase64Encoded":true}
                        """);
    }

    private PartitionTree createPartitionTreeWithRowsPerPartition(int recordsPerPartition, List<Row> rows, Schema schema) {
        return createPartitionTreeWithRowsPerPartitionAndTotal(recordsPerPartition, rows.size(),
                index -> rows.get((int) index),
                schema);
    }
}
