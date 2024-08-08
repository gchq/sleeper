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
package sleeper.core.statestore.commit;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SplitPartitionCommitRequestSerDeTest {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final SplitPartitionCommitRequestSerDe serDe = new SplitPartitionCommitRequestSerDe(schema);

    @Test
    void shouldSerialiseSplitPartitionRequest() {
        // Given
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "aaa")
                .buildTree();
        SplitPartitionCommitRequest splitPartitionCommitRequest = new SplitPartitionCommitRequest(
                "test-table",
                partitionTree.getRootPartition(),
                partitionTree.getPartition("left"), partitionTree.getPartition("right"));

        // When
        String json = serDe.toJsonPrettyPrint(splitPartitionCommitRequest);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(splitPartitionCommitRequest);
        Approvals.verify(json);
    }

    @Test
    void shouldFailToDeserialiseNonSplitPartitionCommitRequest() {
        assertThatThrownBy(() -> serDe.fromJson("{\"type\": \"OTHER\", \"request\":{}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
