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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.commit.AddFilesToStateStore;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.io.IOException;
import java.util.Iterator;

import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Runs a direct ingest to write random data.
 */
public class WriteRandomDataDirect {

    private WriteRandomDataDirect() {
    }

    public static void writeWithIngestFactory(
            SystemTestPropertyValues systemTestProperties, InstanceIngestSession session) throws IOException {
        StateStoreProvider stateStoreProvider = session.createStateStoreProvider();
        writeWithIngestFactory(
                IngestFactory.builder()
                        .objectFactory(ObjectFactory.noUserJars())
                        .localDir("/mnt/scratch")
                        .stateStoreProvider(stateStoreProvider)
                        .instanceProperties(session.instanceProperties())
                        .hadoopConfiguration(session.hadoopConfiguration())
                        .s3AsyncClient(session.s3Async())
                        .build(),
                addFilesToStateStore(session.sqs(), session.s3(),
                        session.instanceProperties(), session.tableProperties(), stateStoreProvider),
                systemTestProperties, session.tableProperties());
    }

    public static void writeWithIngestFactory(
            IngestFactory ingestFactory, AddFilesToStateStore addFilesToStateStore,
            SystemTestPropertyValues testProperties, TableProperties tableProperties) throws IOException {
        Iterator<Record> recordIterator = WriteRandomData.createRecordIterator(testProperties, tableProperties);

        try (IngestCoordinator<Record> ingestCoordinator = ingestFactory.ingestCoordinatorBuilder(tableProperties)
                .addFilesToStateStore(addFilesToStateStore)
                .build()) {
            new IngestRecordsFromIterator(ingestCoordinator, recordIterator).write();
        } catch (StateStoreException | IteratorCreationException e) {
            throw new IOException("Failed to write records using iterator", e);
        }
    }

    private static AddFilesToStateStore addFilesToStateStore(
            AmazonSQS sqsClient, AmazonS3 s3Client, InstanceProperties instanceProperties, TableProperties tableProperties,
            StateStoreProvider stateStoreProvider) {
        if (tableProperties.getBoolean(INGEST_FILES_COMMIT_ASYNC)) {
            return AddFilesToStateStore.bySqs(sqsClient, s3Client, instanceProperties,
                    requestBuilder -> requestBuilder.tableId(tableProperties.get(TABLE_ID)));
        } else {
            return AddFilesToStateStore.synchronous(stateStoreProvider.getStateStore(tableProperties));
        }
    }
}
