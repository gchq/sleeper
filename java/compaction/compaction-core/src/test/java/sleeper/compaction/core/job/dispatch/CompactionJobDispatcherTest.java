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
package sleeper.compaction.core.job.dispatch;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

public class CompactionJobDispatcherTest {

    Schema schema = schemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
    FileReferenceFactory factory = FileReferenceFactory.from(stateStore);

    Map<String, String> s3PathToContent = new HashMap<>();

    @Test
    @Disabled
    void shouldSendCompactionJobsInABatchWhenAllFilesAreAssigned() {

        // Given
        String batchKey = "";
        CompactionJobDispatchCreationHelper.populateBucketWithBatch(batchKey, null);
        CompactionJobDispatchRequest request = buildS3Message(batchKey);

        // When

        // Then
        // assert on status store after
        // assert that compaction jobs are on queue

    }

    private CompactionJobDispatchRequest buildS3Message(String batchKey) {
        return null;
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private void notesMethod() {
        /*
         * Message from queue points to batch in s3 <? possible failure case
         * List of compaction jobs from s3 file <?
         * should be created if input files have been assigned <
         * compactions jobs sent to compaction queue and created in status store
         * log out sucess of batch [info level]
         * if unassigned
         * determine age of batch
         * if young enough
         * whole message returned to queue <
         * if too old
         * dead letter/ throw exception (to determine) <
         */

        //instanceProperties.set(DATA_BUCKET, "sleeper-" + id + "-table-data");

    }
}
