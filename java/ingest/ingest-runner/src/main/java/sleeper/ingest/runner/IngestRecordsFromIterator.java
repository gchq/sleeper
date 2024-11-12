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
package sleeper.ingest.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.impl.IngestCoordinator;

import java.io.IOException;
import java.util.Iterator;

/**
 * Writes records to the storage system from an iterator, partitioned and sorted. This class is an adapter to
 * {@link IngestCoordinator}.
 */
public class IngestRecordsFromIterator {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRecordsFromIterator.class);

    private final Iterator<Record> recordsIterator;
    private final IngestRecords ingestRecords;

    public IngestRecordsFromIterator(IngestCoordinator<Record> ingestCoordinator, Iterator<Record> recordsIterator) {
        this.recordsIterator = recordsIterator;
        this.ingestRecords = new IngestRecords(ingestCoordinator);
    }

    public IngestResult write() throws StateStoreException, IteratorCreationException, IOException {
        ingestRecords.init();
        long count = 0L;
        while (recordsIterator.hasNext()) {
            ingestRecords.write(recordsIterator.next());
            count++;
        }
        LOGGER.info("Ingested {} records", count);
        return ingestRecords.close();
    }
}
