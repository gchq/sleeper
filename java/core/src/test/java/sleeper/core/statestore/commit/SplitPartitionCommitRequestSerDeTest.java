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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SplitPartitionCommitRequestSerDeTest {

    //private final SplitPartitionCommitRequest splitPartitionCommitRequest = new SplitPartitionCommitRequest(null, null, null);

    @Disabled
    @Test
    void shouldSerialiseSplitPartitionRequest() {

        PartitionTree partitionTree = new PartitionsBuilder(schemaWithKey("key", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "aaa")
                .buildTree();

        //Given
        SplitPartitionCommitRequest splitPartitionCommitRequest = new SplitPartitionCommitRequest(partitionTree.getRootPartition(),
                partitionTree.getPartition("left"), partitionTree.getPartition("right"));

        SplitPartitionCommitRequestSerDe serDe = new SplitPartitionCommitRequestSerDe();
        //When
        String json = serDe.toJson(splitPartitionCommitRequest);

        //Then
        assertThat(serDe.fromJson(json)).isEqualTo(splitPartitionCommitRequest);
        Approvals.verify(json);
    }
    /*
     * 
     * @Test
     * void shouldFailToDeserialiseNonStoredInS3CommitRequest() {
     * assertThatThrownBy(() -> serDe.fromJson("{\"type\": \"OTHER\", \"request\":{}}"))
     * .isInstanceOf(IllegalArgumentException.class);
     * }
     */

}
