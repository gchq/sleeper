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

package sleeper.ingest;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.statestore.FileReference;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A data structure to store the results of an ingest. Stores file references which were created, and the records
 * read/written.
 */
public class IngestResult {
    private final List<FileReference> fileReferenceList;
    private final long recordsRead;
    private final long recordsWritten;

    private IngestResult(List<FileReference> fileReferenceList, long recordsRead, long recordsWritten) {
        this.fileReferenceList = fileReferenceList;
        this.recordsRead = recordsRead;
        this.recordsWritten = recordsWritten;
    }

    /**
     * Creates an instance of this class where all records in all files were read and written.
     *
     * @param  fileReferenceList the file reference list
     * @return                   an instance of this class
     */
    public static IngestResult allReadWereWritten(List<FileReference> fileReferenceList) {
        long recordsWritten = recordsWritten(fileReferenceList);
        return new IngestResult(fileReferenceList, recordsWritten, recordsWritten);
    }

    /**
     * Creates an instance of this class where the provided number of records were read, and all records in all files
     * were written.
     *
     * @param  recordsRead       the number of records read
     * @param  fileReferenceList the file reference list
     * @return                   an instance of this class
     */
    public static IngestResult fromReadAndWritten(long recordsRead, List<FileReference> fileReferenceList) {
        return new IngestResult(fileReferenceList, recordsRead, recordsWritten(fileReferenceList));
    }

    /**
     * Creates an instance of this class where no files were created, and no records were read or written.
     *
     * @return an instance of this class
     */
    public static IngestResult noFiles() {
        return new IngestResult(Collections.emptyList(), 0, 0);
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public List<FileReference> getFileReferenceList() {
        return Collections.unmodifiableList(fileReferenceList);
    }

    /**
     * Creates a records processed object from this class.
     *
     * @return a {@link RecordsProcessed} object
     */
    public RecordsProcessed asRecordsProcessed() {
        return new RecordsProcessed(recordsRead, recordsWritten);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestResult that = (IngestResult) o;
        return fileReferenceList.equals(that.fileReferenceList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileReferenceList);
    }

    @Override
    public String toString() {
        return "IngestResult{" +
                "recordsWritten=" + recordsWritten +
                ",fileReferenceList=" + fileReferenceList +
                '}';
    }

    private static long recordsWritten(List<FileReference> fileReferenceList) {
        return fileReferenceList.stream()
                .mapToLong(FileReference::getNumberOfRecords)
                .sum();
    }
}
