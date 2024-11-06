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

import sleeper.ingest.batcher.core.IngestBatcherQueueClient;
import sleeper.ingest.core.job.IngestJob;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMemoryIngestBatcherQueues implements IngestBatcherQueueClient {

    private final Map<String, List<Object>> messagesByQueueUrl = new LinkedHashMap<>();

    @Override
    public void send(String queueUrl, IngestJob job) {
        messagesByQueueUrl.computeIfAbsent(queueUrl, url -> new ArrayList<>())
                .add(job);
    }

    public Map<String, List<Object>> getMessagesByQueueUrl() {
        return messagesByQueueUrl;
    }
}
