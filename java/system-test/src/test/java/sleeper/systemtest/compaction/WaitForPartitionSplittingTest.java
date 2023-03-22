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

package sleeper.systemtest.compaction;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.statestore.StateStore;
import sleeper.statestore.inmemory.StateStoreTestBuilder;

public class WaitForPartitionSplittingTest {
    @Test
    @Disabled("TODO")
    void shouldWaitWhenOnePartitionStillNeedsSplitting() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();
        StateStore stateStore = StateStoreTestBuilder.from(new PartitionsBuilder(schema).singlePartition("root"))
                .partitionFileWithRecords("root", "test.parquet", 123456789L)
                .buildStateStore();

        // When
        WaitForPartitionSplitting waitForPartitionSplitting = new WaitForPartitionSplitting(stateStore);
    }
}
