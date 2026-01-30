/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.compaction.task.InMemoryCompactionTaskTracker;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.task.InMemoryIngestTaskTracker;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class WaitForJobsTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance();
    IngestJobTracker ingestJobTracker = new InMemoryIngestJobTracker();
    IngestTaskTracker ingestTaskTracker = new InMemoryIngestTaskTracker();
    CompactionJobTracker compactionJobTracker = new InMemoryCompactionJobTracker();
    CompactionTaskTracker compactionTaskTracker = new InMemoryCompactionTaskTracker();

    @Test
    void shouldWaitForSuccessfulIngest() {
        // TODO
    }

}
