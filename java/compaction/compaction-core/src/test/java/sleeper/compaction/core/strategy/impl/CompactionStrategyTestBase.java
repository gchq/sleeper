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
package sleeper.compaction.core.strategy.impl;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.strategy.CompactionStrategy;
import sleeper.compaction.core.strategy.CompactionStrategyIndex;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public abstract class CompactionStrategyTestBase {
    protected static final Schema DEFAULT_SCHEMA = schemaWithKey("key");
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, DEFAULT_SCHEMA);
    protected final PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
            .singlePartition("root")
            .buildTree();
    protected final FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(partitionTree);
    protected CompactionStrategy strategy;

    protected List<CompactionJob> createCompactionJobs(List<FileReference> fileReferences, List<Partition> partitions) {
        return createCompactionJobs(jobFactoryWithIncrementingJobIds(), fileReferences, partitions);
    }

    protected List<CompactionJob> createCompactionJobs(CompactionJobFactory jobFactory, List<FileReference> fileReferences, List<Partition> partitions) {
        return strategy.createCompactionJobs(instanceProperties, tableProperties, jobFactory,
                new CompactionStrategyIndex(tableProperties.getStatus(), fileReferences, partitions));
    }

    protected CompactionJobFactory jobFactoryWithIncrementingJobIds() {
        return new CompactionJobFactory(instanceProperties, tableProperties, incrementingJobIds());
    }

    private static Supplier<String> incrementingJobIds() {
        return IntStream.iterate(1, i -> i + 1)
                .mapToObj(i -> "job" + i)
                .iterator()::next;
    }
}
