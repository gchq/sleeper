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
import sleeper.core.range.Range;
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
        PartitionFactory factory = new PartitionFactory(schema);
        Partition parent = factory.partition("parent", "", null);
        Partition child = factory.child(parent, "child", "", "aaa");

        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(parent).isEqualTo(
                new Partition(rowKeyTypes, new Region(new Range(key, "", null)),
                        "parent", false, null, Collections.singletonList("child"), 0));
        assertThat(child).isEqualTo(
                new Partition(rowKeyTypes, new Region(new Range(key, "", "aaa")),
                        "child", true, "parent", Collections.emptyList(), -1));
    }

    @Test
    public void canSpecifyChildrenThenParentPartition() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        PartitionFactory factory = new PartitionFactory(schema);
        Partition a = factory.partition("A", "", "aaa");
        Partition b = factory.partition("B", "aaa", null);
        Partition parent = factory.parentJoining("parent", a, b);

        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(a).isEqualTo(
                new Partition(rowKeyTypes, new Region(new Range(key, "", "aaa")),
                        "A", true, "parent", Collections.emptyList(), -1));
        assertThat(b).isEqualTo(
                new Partition(rowKeyTypes, new Region(new Range(key, "aaa", null)),
                        "B", true, "parent", Collections.emptyList(), -1));
        assertThat(parent).isEqualTo(
                new Partition(rowKeyTypes, new Region(new Range(key, "", null)),
                        "parent", false, null, Arrays.asList("A", "B"), 0));
    }
}
