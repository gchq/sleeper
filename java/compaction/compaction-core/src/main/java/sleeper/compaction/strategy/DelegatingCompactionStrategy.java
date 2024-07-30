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
package sleeper.compaction.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.strategy.CompactionStrategyIndex.FilesInPartition;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A wrapper containing behaviour common to compaction strategies. Delegates to {@link LeafPartitionCompactionStrategy}
 * and {@link ShouldCreateJobsStrategy}.
 */
public class DelegatingCompactionStrategy implements CompactionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingCompactionStrategy.class);

    private final LeafPartitionCompactionStrategy leafStrategy;
    private final ShouldCreateJobsStrategy shouldCreateJobsStrategy;

    public DelegatingCompactionStrategy(LeafPartitionCompactionStrategy leafStrategy) {
        this.leafStrategy = leafStrategy;
        this.shouldCreateJobsStrategy = ShouldCreateJobsStrategy.yes();
    }

    public DelegatingCompactionStrategy(
            LeafPartitionCompactionStrategy leafStrategy, ShouldCreateJobsStrategy shouldCreateJobsStrategy) {
        this.leafStrategy = leafStrategy;
        this.shouldCreateJobsStrategy = shouldCreateJobsStrategy;
    }

    public List<CompactionJob> createCompactionJobs(InstanceProperties instanceProperties, TableProperties tableProperties,
            CompactionJobFactory factory, CompactionStrategyIndex index) {
        leafStrategy.init(instanceProperties, tableProperties, factory);
        shouldCreateJobsStrategy.init(instanceProperties, tableProperties);

        List<CompactionJob> compactionJobs = new ArrayList<>();
        for (FilesInPartition filesInPartition : index.getFilesInLeafPartitions()) {
            compactionJobs.addAll(createJobsForLeafPartition(filesInPartition));
        }
        return compactionJobs;
    }

    private List<CompactionJob> createJobsForLeafPartition(FilesInPartition filesInPartition) {
        long maxNumberOfJobsToCreate = shouldCreateJobsStrategy.maxCompactionJobsToCreate(filesInPartition);
        if (maxNumberOfJobsToCreate < 1) {
            return Collections.emptyList();
        }
        LOGGER.info("Max jobs to create = {}", maxNumberOfJobsToCreate);
        List<CompactionJob> jobs = leafStrategy.createJobsForLeafPartition(filesInPartition);
        LOGGER.info("Defined {} compaction job{} for partition {}, table {}",
                jobs.size(), 1 == jobs.size() ? "" : "s", filesInPartition.getPartitionId(), filesInPartition.getTableStatus());
        while (jobs.size() > maxNumberOfJobsToCreate) {
            jobs.remove(jobs.size() - 1);
        }
        LOGGER.info("Created {} compaction job{} for partition {}, table {}",
                jobs.size(), 1 == jobs.size() ? "" : "s", filesInPartition.getPartitionId(), filesInPartition.getTableStatus());
        return jobs;
    }
}
