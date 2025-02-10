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

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.IngestRecordsFromIterator;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.commit.AddFilesToStateStore;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;
import sleeper.systemtest.configuration.SystemTestPropertyValues;

import java.io.IOException;
import java.util.Iterator;

import static sleeper.core.properties.table.TableProperty.INGEST_FILES_COMMIT_ASYNC;

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
                        .stateStoreProvider(session.stateStoreProvider())
                        .instanceProperties(session.instanceProperties())
                        .hadoopConfiguration(session.hadoopConfiguration())
                        .s3AsyncClient(session.s3Async())
                        .build(),
                addFilesToStateStore(session),
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
        } catch (IteratorCreationException e) {
            throw new IOException("Failed to write records using iterator", e);
        }
    }

    private static AddFilesToStateStore addFilesToStateStore(InstanceIngestSession session) {
        if (session.tableProperties().getBoolean(INGEST_FILES_COMMIT_ASYNC)) {
            return AddFilesToStateStore.bySqs(session.tableProperties(),
                    new SqsFifoStateStoreCommitRequestSender(session.instanceProperties(), session.sqs(), session.s3(),
                            TransactionSerDeProvider.forOneTable(session.tableProperties())),
                    transactionBuilder -> {
                    });
        } else {
            return AddFilesToStateStore.synchronous(session.stateStoreProvider().getStateStore(session.tableProperties()));
        }
    }
}
