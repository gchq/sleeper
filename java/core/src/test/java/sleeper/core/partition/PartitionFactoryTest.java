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
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionFactoryTest {

    @Test
    public void canSpecifyParentThenChildPartition() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        PartitionFactory partitionFactory = new PartitionFactory(schema);
        Partition parent = partitionFactory.partition("parent", "", null);
        Partition child = partitionFactory.child(parent, "child", "", "aaa");

        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(parent).isEqualTo(
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(key, "", null)),
                        "parent", false, null, Collections.singletonList("child"), 0));
        assertThat(child).isEqualTo(
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(key, "", "aaa")),
                        "child", true, "parent", Collections.emptyList(), -1));
    }

    @Test
    public void canSpecifyChildrenThenParentPartition() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        PartitionFactory partitionFactory = new PartitionFactory(schema);
        Partition a = partitionFactory.partition("A", "", "aaa");
        Partition b = partitionFactory.partition("B", "aaa", null);
        Partition parent = partitionFactory.parentJoining("parent", a, b);

        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(a).isEqualTo(
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(key, "", "aaa")),
                        "A", true, "parent", Collections.emptyList(), -1));
        assertThat(b).isEqualTo(
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(key, "aaa", null)),
                        "B", true, "parent", Collections.emptyList(), -1));
        assertThat(parent).isEqualTo(
                new Partition(rowKeyTypes, new Region(rangeFactory.createRange(key, "", null)),
                        "parent", false, null, Arrays.asList("A", "B"), 0));
    }
}
