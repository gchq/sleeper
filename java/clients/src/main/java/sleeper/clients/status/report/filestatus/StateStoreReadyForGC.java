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

import sleeper.statestore.FileLifecycleInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class StateStoreReadyForGC {
    private final List<FileLifecycleInfo> files;
    private final boolean reachedMax;

    private StateStoreReadyForGC(List<FileLifecycleInfo> files, boolean reachedMax) {
        this.files = files;
        this.reachedMax = reachedMax;
    }

    public List<FileLifecycleInfo> getFiles() {
        return files;
    }

    public Stream<FileLifecycleInfo> stream() {
        return files.stream();
    }

    public boolean isReachedMax() {
        return reachedMax;
    }

    public static StateStoreReadyForGC from(StateStore stateStore, int maxNumberOfReadyForGCFilesToCount) throws StateStoreException {
        Iterator<FileLifecycleInfo> readyForGCIT = stateStore.getReadyForGCFileInfos();
        List<FileLifecycleInfo> readyForGC = new ArrayList<>();
        int count = 0;
        while (readyForGCIT.hasNext() && count < maxNumberOfReadyForGCFilesToCount) {
            readyForGC.add(readyForGCIT.next());
            count++;
        }
        boolean reachedMax = count == maxNumberOfReadyForGCFilesToCount;
        return new StateStoreReadyForGC(readyForGC, reachedMax);
    }

    public static StateStoreReadyForGC none() {
        return new StateStoreReadyForGC(Collections.emptyList(), true);
    }
}
