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
package sleeper.ingest.impl;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.IngestResult;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.Iterator;

public class AllTablesIngestFactory {

    private final IngestFactory factory;
    private final TablePropertiesProvider tablePropertiesProvider;

    AllTablesIngestFactory(IngestFactory factory, TablePropertiesProvider tablePropertiesProvider) {
        this.factory = factory;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    public IngestResult ingestRecordsFromIterator(String tableName, Iterator<Record> recordIterator)
            throws StateStoreException, IteratorException, IOException {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
        return factory.ingestRecordsFromIterator(tableProperties, recordIterator);
    }
}
