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
package sleeper.clients.status.report.filestatus;

import sleeper.core.statestore.FileReferences;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StateStoreReadyForGC {

    private final List<String> files;
    private final boolean reachedMax;

    private StateStoreReadyForGC(Set<String> files, boolean reachedMax) {
        this.files = new ArrayList<>(files);
        this.reachedMax = reachedMax;
    }

    public List<String> getFiles() {
        return files;
    }

    public Stream<String> stream() {
        return files.stream();
    }

    public boolean isReachedMax() {
        return reachedMax;
    }

    public static StateStoreReadyForGC from(StateStore stateStore, int maxNumberOfReadyForGCFilesToCount) throws StateStoreException {
        Set<String> readyForGCFilenames = stateStore.getAllFileReferences().getFiles()
                .stream().filter(fileReferences -> fileReferences.getReferences().isEmpty())
                .map(FileReferences::getFilename)
                .limit(maxNumberOfReadyForGCFilesToCount)
                .collect(Collectors.toSet());
        boolean reachedMax = readyForGCFilenames.size() == maxNumberOfReadyForGCFilesToCount;
        return new StateStoreReadyForGC(readyForGCFilenames, reachedMax);
    }

    public static StateStoreReadyForGC none() {
        return new StateStoreReadyForGC(Collections.emptySet(), true);
    }

}
