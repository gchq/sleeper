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
package sleeper.bulkimport.runner;

import sleeper.core.statestore.FileReference;

import java.util.List;

public class BulkImportJobOutput {

    private final List<FileReference> fileReferences;
    private final Runnable stopSparkContext;
    private final long numRecords;

    public BulkImportJobOutput(List<FileReference> fileReferences, Runnable stopSparkContext) {
        this.fileReferences = fileReferences;
        this.stopSparkContext = stopSparkContext;
        this.numRecords = fileReferences.stream()
                .mapToLong(FileReference::getNumberOfRecords)
                .sum();
    }

    public List<FileReference> fileReferences() {
        return fileReferences;
    }

    public int numFiles() {
        return fileReferences.size();
    }

    public long numRecords() {
        return numRecords;
    }

    public void stopSparkContext() {
        stopSparkContext.run();
    }
}
