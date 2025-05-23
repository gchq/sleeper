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
package sleeper.garbagecollector;

import sleeper.core.table.TableStatus;
import sleeper.garbagecollector.FailedGarbageCollectionException.FileFailure;
import sleeper.garbagecollector.FailedGarbageCollectionException.StateStoreUpdateFailure;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Tracks which files have been deleted from a table by the garbage collector. Used by {@link GarbageCollector}.
 */
public class TableFilesDeleted {

    private final TableStatus table;
    private final List<FileFailure> fileFailures = new ArrayList<>();
    private final List<StateStoreUpdateFailure> stateStoreUpdateFailures = new ArrayList<>();
    private List<String> filesDeletedInBatch = null;
    private int deletedFileCount = 0;

    TableFilesDeleted(TableStatus table) {
        this.table = table;
    }

    void startBatch(List<String> filenames) {
        filesDeletedInBatch = new ArrayList<>(filenames.size());
    }

    public void deleted(String filename) {
        deletedFileCount++;
        filesDeletedInBatch.add(filename);
    }

    public void failed(List<String> filenames, Exception failure) {
        fileFailures.add(new FileFailure(filenames, failure));
    }

    void failedStateStoreUpdate(List<String> filenames, Exception failure) {
        stateStoreUpdateFailures.add(new StateStoreUpdateFailure(filenames, failure));
    }

    List<String> getFilesDeletedInBatch() {
        return filesDeletedInBatch;
    }

    int getDeletedFileCount() {
        return deletedFileCount;
    }

    TableFailures buildTableFailures(Exception tableFailure) {
        return new TableFailures(table, tableFailure, fileFailures, stateStoreUpdateFailures);
    }

    Optional<TableFailures> buildTableFailures() {
        if (fileFailures.isEmpty() && stateStoreUpdateFailures.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(buildTableFailures(null));
        }
    }

}
