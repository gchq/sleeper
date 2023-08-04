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

package sleeper.systemtest.drivers.ingest;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.ingest.IngestFactory;
import sleeper.core.statestore.StateStoreException;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;

public class DirectIngestDriver {
    private final SleeperInstanceContext instance;
    private final Path tempDir;

    public DirectIngestDriver(SleeperInstanceContext instance, Path tempDir) {
        this.instance = instance;
        this.tempDir = tempDir;
    }

    public void ingest(Iterator<Record> records) {
        try {
            factory().ingestFromRecordIterator(instance.getTableProperties(), records);
        } catch (StateStoreException | IteratorException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private IngestFactory factory() {
        return IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(tempDir.toString())
                .stateStoreProvider(instance.getStateStoreProvider())
                .instanceProperties(instance.getInstanceProperties())
                .build();
    }
}
