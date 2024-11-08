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

package sleeper.clients.status.report.ingest.batcher;

import sleeper.clients.status.report.ingest.batcher.query.AllFilesQuery;
import sleeper.clients.status.report.ingest.batcher.query.BatcherQueryPrompt;
import sleeper.clients.status.report.ingest.batcher.query.PendingFilesQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.ingest.batcher.core.FileIngestRequest;
import sleeper.ingest.batcher.core.IngestBatcherStore;

import java.util.List;

public interface BatcherQuery {
    List<FileIngestRequest> run(IngestBatcherStore store);

    Type getType();

    static BatcherQuery from(BatcherQuery.Type queryType, ConsoleInput in) {
        if (queryType == Type.PROMPT) {
            return BatcherQueryPrompt.from(in);
        } else if (queryType == Type.ALL) {
            return new AllFilesQuery();
        } else if (queryType == Type.PENDING) {
            return new PendingFilesQuery();
        } else {
            throw new IllegalArgumentException("Unexpected query type: " + queryType);
        }

    }

    enum Type {
        PROMPT,
        ALL,
        PENDING
    }
}
