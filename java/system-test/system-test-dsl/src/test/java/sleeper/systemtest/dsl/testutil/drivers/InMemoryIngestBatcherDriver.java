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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcher;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.ingest.IngestBatcherDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryIngestBatcherDriver implements IngestBatcherDriver {

    private final SystemTestInstanceContext instance;
    private final IngestBatcherStore store;
    private final InMemoryIngestByQueue ingest;
    private final long fileSizeBytes;

    public InMemoryIngestBatcherDriver(SystemTestContext context, IngestBatcherStore store, InMemoryIngestByQueue ingest, long fileSizeBytes) {
        this.instance = context.instance();
        this.store = store;
        this.ingest = ingest;
        this.fileSizeBytes = fileSizeBytes;
    }

    @Override
    public void sendFiles(List<String> files) {
        for (String file : files) {
            store.addFile(FileIngestRequest.builder()
                    .file(file)
                    .fileSizeBytes(fileSizeBytes)
                    .tableId(instance.getTableProperties().get(TABLE_ID))
                    .receivedTime(Instant.now())
                    .build());
        }
    }

    @Override
    public List<String> invokeGetJobIds() {
        List<String> jobIds = new ArrayList<>();
        IngestBatcher.builder()
                .instanceProperties(instance.getInstanceProperties())
                .tablePropertiesProvider(instance.getTablePropertiesProvider())
                .store(store)
                .queueClient((queueUrl, job) -> {
                    ingest.send(job);
                    jobIds.add(job.getId());
                })
                .build().batchFiles();
        return jobIds;
    }

    @Override
    public void clearStore() {
        store.deleteAllPending();
    }

}
