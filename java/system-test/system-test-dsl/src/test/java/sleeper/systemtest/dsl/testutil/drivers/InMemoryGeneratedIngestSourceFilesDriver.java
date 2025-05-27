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

import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFiles;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;

import java.util.List;

public class InMemoryGeneratedIngestSourceFilesDriver implements GeneratedIngestSourceFilesDriver {
    private final InMemoryRecordStore sourceFiles;

    public InMemoryGeneratedIngestSourceFilesDriver(InMemoryRecordStore sourceFiles) {
        this.sourceFiles = sourceFiles;
    }

    @Override
    public GeneratedIngestSourceFiles findGeneratedFiles() {
        return new GeneratedIngestSourceFiles("in-memory", List.of());
    }

    @Override
    public void emptyBucket() {
        sourceFiles.deleteAllFiles();
    }
}
