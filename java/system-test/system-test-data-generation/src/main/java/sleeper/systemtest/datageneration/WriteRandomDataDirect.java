/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.configuration.SystemTestProperties;

import java.io.IOException;

/**
 * Runs {@link sleeper.ingest.IngestRecordsFromIterator} to write random data.
 */
public class WriteRandomDataDirect {

    private WriteRandomDataDirect() {
    }

    public static void writeWithIngestFactory(SystemTestProperties properties,
                                              TableProperties tableProperties,
                                              StateStoreProvider stateStoreProvider) throws IOException {
        writeWithIngestFactory(
                IngestFactory.builder()
                        .objectFactory(ObjectFactory.noUserJars())
                        .localDir("/mnt/scratch")
                        .stateStoreProvider(stateStoreProvider)
                        .instanceProperties(properties)
                        .build(),
                properties, tableProperties);
    }

    public static void writeWithIngestFactory(IngestFactory ingestFactory,
                                              SystemTestProperties properties,
                                              TableProperties tableProperties) throws IOException {
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        CloseableIterator<Record> recordIterator = new WrappedIterator<>(
                WriteRandomData.createRecordIterator(properties, tableProperties));

        try {
            ingestFactory.ingestFromRecordIteratorAndClose(tableProperties, recordIterator);
        } catch (StateStoreException | IteratorException e) {
            throw new IOException("Failed to write records using iterator", e);
        }

        dynamoDBClient.shutdown();
    }
}
