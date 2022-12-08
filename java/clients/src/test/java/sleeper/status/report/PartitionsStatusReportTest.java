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

package sleeper.status.report;

import org.junit.Test;
import sleeper.ToStringPrintStream;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionFactory;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.status.report.partitions.PartitionsQuery;
import sleeper.status.report.partitions.StandardPartitionsStatusReporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.ClientTestUtils.example;

public class PartitionsStatusReportTest {
    private final StateStore stateStore = mock(StateStore.class);

    @Test
    public void shouldReportRootPartitionWithNoLeaves() throws StateStoreException, IOException {
        // Given
        when(stateStore.getAllPartitions()).thenReturn(createRootPartitionWithNoLeaves());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithNoLeaves.txt"));
    }

    @Test
    public void shouldReportRootPartitionWithTwoLeaves() throws StateStoreException, IOException {
        // Given
        when(stateStore.getAllPartitions()).thenReturn(createRootPartitionWithTwoLeaves());

        // When
        assertThat(getStandardReport()).hasToString(
                example("reports/partitions/rootWithTwoLeaves.txt"));
    }

    private List<Partition> createRootPartitionWithTwoLeaves() {
        PartitionFactory partitionFactory = createPartitionFactory();
        Partition a = partitionFactory.partition("A", "", "aaa");
        Partition b = partitionFactory.partition("B", "aaa", null);
        Partition parent = partitionFactory.parentJoining("parent", a, b);
        return Arrays.asList(parent, a, b);
    }

    private List<Partition> createRootPartitionWithNoLeaves() {
        PartitionFactory partitionFactory = createPartitionFactory();
        Partition root = partitionFactory.partition("root", "", null);
        return Collections.singletonList(root);
    }

    private PartitionFactory createPartitionFactory() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        return new PartitionFactory(schema);
    }

    private String getStandardReport() throws StateStoreException {
        ToStringPrintStream output = new ToStringPrintStream();
        StandardPartitionsStatusReporter reporter = new StandardPartitionsStatusReporter(output.getPrintStream());
        new PartitionsStatusReport(stateStore, reporter, PartitionsQuery.ALL).run();
        return output.toString();
    }
}
