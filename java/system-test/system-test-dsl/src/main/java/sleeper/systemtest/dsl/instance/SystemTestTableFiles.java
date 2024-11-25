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

package sleeper.systemtest.dsl.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.PollWithRetries;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class SystemTestTableFiles {
    public static final Logger LOGGER = LoggerFactory.getLogger(SystemTestTableFiles.class);

    private final SystemTestInstanceContext instance;

    public SystemTestTableFiles(SystemTestInstanceContext instance) {
        this.instance = instance;
    }

    public AllReferencesToAllFiles all() {
        return instance.getStateStore().getAllFilesWithMaxUnreferenced(10000);
    }

    public List<FileReference> references() {
        return instance.getStateStore().getFileReferences();
    }

    public Map<String, Long> recordsByFilename() {
        return all().recordsByFilename();
    }

    public SystemTestTableFiles waitForState(Predicate<AllReferencesToAllFiles> stateCheck, PollWithRetries poll) throws InterruptedException {
        poll.pollUntil("files meet expected state", () -> {
            AllReferencesToAllFiles files = all();
            LOGGER.info("Found {} referenced files, {} unreferenced",
                    files.getFilesWithReferences().size(),
                    files.getFilesWithNoReferences().size());
            return stateCheck.test(files);
        });
        return this;
    }

    public Map<String, AllReferencesToAllFiles> filesByTable() {
        return instance.streamTableProperties().parallel()
                .map(this::getFiles)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, List<FileReference>> referencesByTable() {
        return instance.streamTableProperties().parallel()
                .map(this::getReferences)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map.Entry<String, List<FileReference>> getReferences(TableProperties properties) {
        StateStore stateStore = instance.getStateStore(properties);
        return entry(properties.get(TABLE_NAME), stateStore.getFileReferences());
    }

    private Map.Entry<String, AllReferencesToAllFiles> getFiles(TableProperties properties) {
        StateStore stateStore = instance.getStateStore(properties);
        return entry(properties.get(TABLE_NAME),
                stateStore.getAllFilesWithMaxUnreferenced(100));
    }
}
