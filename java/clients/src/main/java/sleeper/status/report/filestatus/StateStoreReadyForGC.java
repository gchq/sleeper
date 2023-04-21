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
package sleeper.status.report.filestatus;

import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class StateStoreReadyForGC {
    private final List<FileInfo> files;
    private final boolean reachedMax;

    private StateStoreReadyForGC(List<FileInfo> files, boolean reachedMax) {
        this.files = files;
        this.reachedMax = reachedMax;
    }

    public List<FileInfo> getFiles() {
        return files;
    }

    public Stream<FileInfo> stream() {
        return files.stream();
    }

    public boolean isReachedMax() {
        return reachedMax;
    }

    public static StateStoreReadyForGC from(StateStore stateStore, int maxNumberOfReadyForGCFilesToCount) throws StateStoreException {
        Iterator<FileInfo> readyForGCIT = stateStore.getReadyForGCFileInfos();
        List<FileInfo> readyForGC = new ArrayList<>();
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
