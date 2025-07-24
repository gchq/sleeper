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
package sleeper.ingest.runner;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.row.Row;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.impl.IngestCoordinator;

import java.io.IOException;

/**
 * Writes rows to the storage system, partitioned and sorted. This class is an adapter to {@link IngestCoordinator}.
 */
public class IngestRows {

    private final IngestCoordinator<Row> ingestCoordinator;

    public IngestRows(IngestCoordinator<Row> ingestCoordinator) {
        this.ingestCoordinator = ingestCoordinator;
    }

    public void init() {
        // Do nothing
    }

    public void write(Row row) throws IOException, IteratorCreationException, StateStoreException {
        ingestCoordinator.write(row);
    }

    public IngestResult close() throws StateStoreException, IteratorCreationException, IOException {
        return ingestCoordinator.closeReturningResult();
    }
}
