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
package sleeper.systemtest.datageneration;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.io.IOException;

/**
 * Runs a direct ingest to write random data.
 */
public class WriteRandomDataDirect {

    private WriteRandomDataDirect() {
    }

    public static void writeWithIngestFactory(
            SystemTestPropertyValues systemTestProperties, InstanceIngestSession session) throws IOException {
        writeWithIngestFactory(
                IngestFactory.builder()
                        .objectFactory(ObjectFactory.noUserJars())
                        .localDir("/mnt/scratch")
                        .stateStoreProvider(session.createStateStoreProvider())
                        .instanceProperties(session.instanceProperties())
                        .hadoopConfiguration(session.hadoopConfiguration())
                        .s3AsyncClient(session.s3Async())
                        .build(),
                systemTestProperties, session.tableProperties());
    }

    public static void writeWithIngestFactory(
            IngestFactory ingestFactory, SystemTestPropertyValues properties, TableProperties tableProperties) throws IOException {
        CloseableIterator<Record> recordIterator = new WrappedIterator<>(
                WriteRandomData.createRecordIterator(properties, tableProperties));

        try {
            ingestFactory.ingestFromRecordIteratorAndClose(tableProperties, recordIterator);
        } catch (StateStoreException | IteratorCreationException e) {
            throw new IOException("Failed to write records using iterator", e);
        }
    }
}
