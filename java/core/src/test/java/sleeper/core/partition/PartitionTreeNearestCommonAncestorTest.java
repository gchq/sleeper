/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import sleeper.core.key.Key;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTreeNearestCommonAncestorTest {

    @Test
    public void shouldTellWhenBothPartitionsAreTheSame() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema, Collections.emptyList());
        PartitionTree tree = new PartitionTree(schema, partitions);
        Partition partition = partitions.get(0);

        assertThat(tree.getNearestCommonAncestor(Key.create("a"), Key.create("b")))
                .isEqualTo(partition);
    }

    @Test
    public void shouldGetRootPartitionForTwoImmediatelyBeneathIt() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema, Collections.singletonList("abc"));
        PartitionTree tree = new PartitionTree(schema, partitions);

        assertThat(tree.getNearestCommonAncestor(Key.create("a"), Key.create("b")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetRootPartitionForTwoInSeparatedPartitions() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema, Arrays.asList("abc", "def"));
        PartitionTree tree = new PartitionTree(schema, partitions);

        assertThat(tree.getNearestCommonAncestor(Key.create("a"), Key.create("z")))
                .isEqualTo(tree.getRootPartition());
    }
}
