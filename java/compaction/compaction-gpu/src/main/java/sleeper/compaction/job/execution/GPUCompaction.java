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
package sleeper.compaction.job.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionRunner;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.range.Region;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;

import java.time.LocalDateTime;
import java.util.NoSuchElementException;
import java.util.Objects;

public class GPUCompaction implements CompactionRunner {
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    private static final Logger LOGGER = LoggerFactory.getLogger(GPUCompaction.class);

    public GPUCompaction(TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
    }

    @Override
    public RecordsProcessed compact(CompactionJob job) throws Exception {
        TableProperties tableProperties = tablePropertiesProvider
                .getById(job.getTableId());
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Region region = stateStore.getAllPartitions().stream()
                .filter(p -> Objects.equals(job.getPartitionId(), p.getId()))
                .findFirst().orElseThrow(() -> new NoSuchElementException("Partition not found for compaction job"))
                .getRegion();

        RecordsProcessed result = new RecordsProcessed(0, 0);

        LOGGER.info("Compaction job {}: compaction finished at {}", job.getId(),
                LocalDateTime.now());
        return result;
    }

    @Override
    public String implementationLanguage() {
        return "C++";
    }

    @Override
    public boolean isHardwareAccelerated() {
        return true;
    }

    @Override
    public boolean supportsIterators() {
        return false;
    }
}
