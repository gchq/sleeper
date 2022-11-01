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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.List;

/**
 * Writes a {@link Record} objects to the storage system, partitioned and sorted.
 * <p>
 * This class is an adaptor to {@link IngestCoordinator}.
 */
public class IngestRecords {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRecords.class);

    private final IngestCoordinator<Record> ingestCoordinator;

    public IngestRecords(IngestProperties properties) {
        if (null == properties.getHadoopConfiguration()) {
            properties = properties.toBuilder().hadoopConfiguration(defaultHadoopConfiguration()).build();
        }
        this.ingestCoordinator = StandardIngestCoordinator.directWriteBackedByArrayList(properties);
    }

    public IngestRecords(IngestCoordinator<Record> ingestCoordinator) {
        this.ingestCoordinator = ingestCoordinator;
    }

    private static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }

    public void init() throws StateStoreException {
        // Do nothing
    }

    public void write(Record record) throws IOException, IteratorException, InterruptedException, StateStoreException {
        ingestCoordinator.write(record);
    }

    public long close() throws IOException, IteratorException, InterruptedException, StateStoreException {
        return ingestCoordinator.closeReturningFileInfoList().stream()
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
    }

    public List<FileInfo> closeReturningFileInfoList() throws IOException, IteratorException, StateStoreException {
        return ingestCoordinator.closeReturningFileInfoList();
    }

    public IngestResult closeWithResult() throws StateStoreException, IteratorException, IOException {
        return IngestResult.from(ingestCoordinator.closeReturningFileInfoList());
    }
}
