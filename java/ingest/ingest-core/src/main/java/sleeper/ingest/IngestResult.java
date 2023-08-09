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

package sleeper.ingest;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.statestore.FileInfo;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IngestResult {
    private final List<FileInfo> fileInfoList;
    private final long recordsRead;
    private final long recordsWritten;

    private IngestResult(List<FileInfo> fileInfoList, long recordsRead, long recordsWritten) {
        this.fileInfoList = fileInfoList;
        this.recordsRead = recordsRead;
        this.recordsWritten = recordsWritten;
    }

    public static IngestResult allReadWereWritten(List<FileInfo> fileInfoList) {
        long recordsWritten = recordsWritten(fileInfoList);
        return new IngestResult(fileInfoList, recordsWritten, recordsWritten);
    }

    public static IngestResult fromReadAndWritten(long recordsRead, List<FileInfo> fileInfoList) {
        return new IngestResult(fileInfoList, recordsRead, recordsWritten(fileInfoList));
    }

    public static IngestResult noFiles() {
        return new IngestResult(Collections.emptyList(), 0, 0);
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    public List<FileInfo> getFileInfoList() {
        return Collections.unmodifiableList(fileInfoList);
    }

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
        return fileInfoList.equals(that.fileInfoList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileInfoList);
    }

    @Override
    public String toString() {
        return "IngestResult{" +
                "recordsWritten=" + recordsWritten +
                ",fileInfoList=" + fileInfoList +
                '}';
    }

    private static long recordsWritten(List<FileInfo> fileInfoList) {
        return fileInfoList.stream()
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
    }
}
