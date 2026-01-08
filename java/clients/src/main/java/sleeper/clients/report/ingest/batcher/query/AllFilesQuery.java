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

package sleeper.clients.report.ingest.batcher.query;

import sleeper.clients.report.ingest.batcher.BatcherQuery;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.util.List;

/**
 * A query to retrieve all file tracking information from the ingest batcher store.
 */
public class AllFilesQuery implements BatcherQuery {

    @Override
    public List<IngestBatcherTrackedFile> run(IngestBatcherStore store) {
        return store.getAllFilesNewestFirst();
    }

    @Override
    public Type getType() {
        return Type.ALL;
    }
}
