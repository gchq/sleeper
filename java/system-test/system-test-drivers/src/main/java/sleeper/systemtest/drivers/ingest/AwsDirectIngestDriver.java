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

package sleeper.systemtest.drivers.ingest;

import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.record.Row;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class AwsDirectIngestDriver implements DirectIngestDriver {
    private final SystemTestInstanceContext instance;
    private final S3AsyncClient s3Async;

    public AwsDirectIngestDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        s3Async = clients.getS3Async();
    }

    public void ingest(Path tempDir, Iterator<Row> records) {
        ingest(records, ingestCoordinatorBuilder(tempDir));
    }

    @Override
    public void ingest(Path tempDir, Iterator<Row> records, Consumer<List<FileReference>> addFiles) {
        ingest(records, ingestCoordinatorBuilder(tempDir)
                .addFilesToStateStore(addFiles::accept));
    }

    private void ingest(Iterator<Row> records, IngestCoordinator.Builder<Row> builder) {
        try (IngestCoordinator<Row> coordinator = builder.build()) {
            while (records.hasNext()) {
                coordinator.write(records.next());
            }
        } catch (IteratorCreationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private IngestCoordinator.Builder<Row> ingestCoordinatorBuilder(Path tempDir) {
        return factory(tempDir).ingestCoordinatorBuilder(instance.getTableProperties());
    }

    private IngestFactory factory(Path tempDir) {
        return IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(tempDir.toString())
                .stateStoreProvider(instance.getStateStoreProvider())
                .instanceProperties(instance.getInstanceProperties())
                .s3AsyncClient(s3Async)
                .build();
    }
}
