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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.sketches.testutils.InMemorySketchesStore;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class InMemoryDirectIngestDriver implements DirectIngestDriver {
    private final SystemTestInstanceContext instance;
    private final InMemoryRowStore data;
    private final InMemorySketchesStore sketches;

    public InMemoryDirectIngestDriver(SystemTestInstanceContext instance, InMemoryRowStore data, InMemorySketchesStore sketches) {
        this.instance = instance;
        this.data = data;
        this.sketches = sketches;
    }

    @Override
    public void ingest(Path tempDir, Iterator<Row> rows) {
        ingest(rows, ingestCoordinatorBuilder(tempDir));
    }

    @Override
    public void ingest(Path tempDir, Iterator<Row> rows, Consumer<List<FileReference>> addFiles) {
        ingest(rows, ingestCoordinatorBuilder(tempDir)
                .addFilesToStateStore(addFiles::accept));
    }

    private void ingest(Iterator<Row> rows, IngestCoordinator.Builder<Row> builder) {
        try (IngestCoordinator<Row> coordinator = builder.build()) {
            while (rows.hasNext()) {
                coordinator.write(rows.next());
            }
        } catch (IteratorCreationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private IngestCoordinator.Builder<Row> ingestCoordinatorBuilder(Path tempDir) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = instance.getTableProperties();
        StateStore stateStore = instance.getStateStore(tableProperties);
        InMemoryIngest ingest = new InMemoryIngest(instanceProperties, tableProperties, stateStore, data, sketches);
        return ingest.coordinatorBuilder();
    }
}
