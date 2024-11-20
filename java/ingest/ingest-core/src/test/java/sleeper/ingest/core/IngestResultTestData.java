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
package sleeper.ingest.core;

import sleeper.core.statestore.FileReferenceTestData;

import java.util.Collections;

/**
 * A helper for creating ingest results for tests.
 */
public class IngestResultTestData {

    private IngestResultTestData() {
    }

    /**
     * Creates an ingest result for a single file on the root partition, where all records were read and written.
     *
     * @param  filename the name of the file
     * @return          an {@link IngestResult}
     */
    public static IngestResult defaultFileIngestResult(String filename) {
        return IngestResult.allReadWereWritten(Collections.singletonList(
                FileReferenceTestData.defaultFileOnRootPartition(filename)));
    }

    /**
     * Creates an ingest result for a single file on the root partition.
     *
     * @param  filename       the name of the file
     * @param  recordsRead    the number of records read
     * @param  recordsWritten the number of records written
     * @return                an {@link IngestResult}
     */
    public static IngestResult defaultFileIngestResultReadAndWritten(
            String filename, long recordsRead, long recordsWritten) {
        return IngestResult.fromReadAndWritten(recordsRead, Collections.singletonList(
                FileReferenceTestData.defaultFileOnRootPartitionWithRecords(filename, recordsWritten)));
    }
}
