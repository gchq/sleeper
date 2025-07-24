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

package sleeper.ingest.core;

import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A data structure to store the results of an ingest. Stores file references which were created, and the rows
 * read/written.
 */
public class IngestResult {
    private final List<FileReference> fileReferenceList;
    private final long rowsRead;
    private final long rowsWritten;

    private IngestResult(List<FileReference> fileReferenceList, long rowsRead, long rowsWritten) {
        this.fileReferenceList = fileReferenceList;
        this.rowsRead = rowsRead;
        this.rowsWritten = rowsWritten;
    }

    /**
     * Creates an instance of this class where all rows in all files were read and written.
     *
     * @param  fileReferenceList the file reference list
     * @return                   an instance of this class
     */
    public static IngestResult allReadWereWritten(List<FileReference> fileReferenceList) {
        long rowsWritten = rowsWritten(fileReferenceList);
        return new IngestResult(fileReferenceList, rowsWritten, rowsWritten);
    }

    /**
     * Creates an instance of this class where the provided number of rows were read, and all rows in all files
     * were written.
     *
     * @param  rowsRead          the number of rows read
     * @param  fileReferenceList the file reference list
     * @return                   an instance of this class
     */
    public static IngestResult fromReadAndWritten(long rowsRead, List<FileReference> fileReferenceList) {
        return new IngestResult(fileReferenceList, rowsRead, rowsWritten(fileReferenceList));
    }

    /**
     * Creates an instance of this class where no files were created, and no rows were read or written.
     *
     * @return an instance of this class
     */
    public static IngestResult noFiles() {
        return new IngestResult(Collections.emptyList(), 0, 0);
    }

    public long getRowsWritten() {
        return rowsWritten;
    }

    public List<FileReference> getFileReferenceList() {
        return Collections.unmodifiableList(fileReferenceList);
    }

    /**
     * Creates a rows processed object from this class.
     *
     * @return a {@link RowsProcessed} object
     */
    public RowsProcessed asRowsProcessed() {
        return new RowsProcessed(rowsRead, rowsWritten);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestResult)) {
            return false;
        }
        IngestResult other = (IngestResult) obj;
        return Objects.equals(fileReferenceList, other.fileReferenceList) && rowsRead == other.rowsRead && rowsWritten == other.rowsWritten;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileReferenceList, rowsRead, rowsWritten);
    }

    @Override
    public String toString() {
        return "IngestResult{fileReferenceList=" + fileReferenceList + ", rowsRead=" + rowsRead + ", rowsWritten=" + rowsWritten + "}";
    }

    private static long rowsWritten(List<FileReference> fileReferenceList) {
        return fileReferenceList.stream()
                .mapToLong(FileReference::getNumberOfRecords)
                .sum();
    }
}
