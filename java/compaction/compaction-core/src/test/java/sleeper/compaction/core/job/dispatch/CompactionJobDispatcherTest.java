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

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

public class CompactionJobDispatcherTest {
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

    Schema schema = schemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    CompactionJobFactory compactionFactory = new CompactionJobFactory(instanceProperties, tableProperties);

    Map<String, List<CompactionJob>> s3PathToCompactionJobBatch = new HashMap<>();
    List<CompactionJob> compactionQueue = new ArrayList<>();

    @Test
    void shouldSendCompactionJobsInABatchWhenAllFilesAreAssigned() throws Exception {

        // Given
        String batchKey = "batch.json";
        FileReference file = fileFactory.rootFile("test.parquet", 1234);
        CompactionJob job = compactionFactory.createCompactionJob("test-job", List.of(file), "root");
        putCompactionJobBatch(batchKey, List.of(job));
        stateStore.addFile(file);
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(job.getId(), job.getPartitionId(), job.getInputFiles())));
        CompactionJobDispatchRequest request = new CompactionJobDispatchRequest(batchKey);

        // When
        dispatcher().dispatch(request);

        // Then
        // assert on status store after
        // assert that compaction jobs are on queue
        assertThat(compactionQueue).containsExactly(job);
    }

    private void putCompactionJobBatch(String key, List<CompactionJob> jobs) {
        s3PathToCompactionJobBatch.put(instanceProperties.get(DATA_BUCKET) + "/" + key, jobs);
    }

    private CompactionJobDispatcher dispatcher() {
        return new CompactionJobDispatcher(instanceProperties, readBatch(), compactionQueue::add);
    }

    private CompactionJobDispatcher.ReadBatch readBatch() {
        return (bucketName, key) -> s3PathToCompactionJobBatch.get(bucketName + "/" + key);
    }
}
