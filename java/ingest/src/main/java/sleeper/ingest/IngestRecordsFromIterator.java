/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest;

import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.Iterator;

/**
 * Writes an {@link Iterator} of {@link Record} objects to the storage system, partitioned and sorted.
 * <p>
 * This class is an adaptor to {@link sleeper.ingest.impl.IngestCoordinator}.
 */
public class IngestRecordsFromIterator {
    private final IngestProperties properties;
    private final Iterator<Record> recordsIterator;

    public IngestRecordsFromIterator(IngestProperties properties, Iterator<Record> recordsIterator) {
        this.properties = properties;
        this.recordsIterator = recordsIterator;
    }

    /**
     * This version of the constructor allows a bespoke Hadoop configuration to be specified. The underlying {@link
     * org.apache.hadoop.fs.FileSystem} object maintains a cache of file systems and the first time that it creates a {@link
     * org.apache.hadoop.fs.s3a.S3AFileSystem} object, the provided Hadoop configuration will be used. Thereafter, the
     * Hadoop configuration will be ignored until {@link org.apache.hadoop.fs.FileSystem#closeAll()} is called. This is not ideal behaviour.
     */


    public long write() throws StateStoreException, IOException, InterruptedException, IteratorException {
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        while (recordsIterator.hasNext()) {
            ingestRecords.write(recordsIterator.next());
        }
        return ingestRecords.close();
    }
}
