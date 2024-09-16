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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SystemTestTableFiles {

    private final SystemTestInstanceContext instance;

    public SystemTestTableFiles(SystemTestInstanceContext instance) {
        this.instance = instance;
    }

    public AllReferencesToAllFiles all() {
        try {
            return instance.getStateStore().getAllFilesWithMaxUnreferenced(10000);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public List<FileReference> references() {
        try {
            return instance.getStateStore().getFileReferences();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
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
        try {
            return entry(properties.get(TABLE_NAME), stateStore.getFileReferences());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private Map.Entry<String, AllReferencesToAllFiles> getFiles(TableProperties properties) {
        StateStore stateStore = instance.getStateStore(properties);
        try {
            return entry(
                    properties.get(TABLE_NAME),
                    stateStore.getAllFilesWithMaxUnreferenced(100));
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
