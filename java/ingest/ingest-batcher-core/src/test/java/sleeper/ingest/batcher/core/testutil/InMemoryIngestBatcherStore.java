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

package sleeper.ingest.batcher.core.testutil;

import sleeper.ingest.batcher.core.FileIngestRequest;
import sleeper.ingest.batcher.core.IngestBatcherStore;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.ingest.batcher.core.testutil.IngestBatcherStoreKeyFields.keyFor;

public class InMemoryIngestBatcherStore implements IngestBatcherStore {

    private final Map<IngestBatcherStoreKeyFields, FileIngestRequest> requests = new LinkedHashMap<>();

    @Override
    public void addFile(FileIngestRequest fileIngestRequest) {
        requests.put(keyFor(fileIngestRequest), fileIngestRequest);
    }

    @Override
    public List<String> assignJobGetAssigned(String jobId, List<FileIngestRequest> filesInJob) {
        filesInJob.forEach(file -> {
            requests.remove(keyFor(file));
            FileIngestRequest fileWithJob = file.toBuilder().jobId(jobId).build();
            requests.put(keyFor(fileWithJob), fileWithJob);
        });
        return filesInJob.stream()
                .map(FileIngestRequest::getFile)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public List<FileIngestRequest> getAllFilesNewestFirst() {
        return requests.values().stream()
                .sorted(Comparator.comparing(FileIngestRequest::getReceivedTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public List<FileIngestRequest> getPendingFilesOldestFirst() {
        return requests.values().stream()
                .filter(request -> !request.isAssignedToJob())
                .sorted(Comparator.comparing(FileIngestRequest::getReceivedTime))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteAllPending() {
        getPendingFilesOldestFirst().forEach(fileIngestRequest -> requests.remove(keyFor(fileIngestRequest)));
    }
}
