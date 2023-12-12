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
package sleeper.compaction.strategy.impl;

import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.strategy.DelegatingCompactionStrategy;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;

/**
 * A simple {@link sleeper.compaction.strategy.CompactionStrategy} that lists the active files for a partition in increasing order of the number
 * of records they contain, and iterates through this list creating compaction jobs with at most
 * maximumNumberOfFilesToCompact files in each.
 */
public class BasicCompactionStrategy extends DelegatingCompactionStrategy {

    @Override
    public void init(InstanceProperties instanceProperties, TableProperties tableProperties, boolean forceCreateJobs) {
        factory = new CompactionJobFactory(instanceProperties, tableProperties);
        leafStrategy.init(instanceProperties, tableProperties, factory, forceCreateJobs);
        shouldCreateJobsStrategy.init(instanceProperties, tableProperties);
        schema = tableProperties.getSchema();
        tableName = tableProperties.get(TableProperty.TABLE_NAME);
        compactionFilesBatchSize = tableProperties.getInt(COMPACTION_FILES_BATCH_SIZE);
    }


    public BasicCompactionStrategy() {
        super(new BasicLeafStrategy());
    }

}
