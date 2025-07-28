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
package sleeper.systemtest.suite.investigate;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;

import java.util.List;
import java.util.Optional;

public record CompactionChangedRowCount(ReplaceFileReferencesRequest job, List<FileReference> inputFiles, FileReference outputFile) {

    public static List<CompactionChangedRowCount> detectChanges(
            ReplaceFileReferencesTransaction transaction, TransactionLogEntryHandle entry, StateStoreFiles state) {
        return transaction.getJobs().stream()
                .flatMap(job -> detectChange(job, entry.original(), state).stream())
                .toList();
    }

    private static Optional<CompactionChangedRowCount> detectChange(ReplaceFileReferencesRequest job, TransactionLogEntry entry, StateStoreFiles state) {
        List<FileReference> inputFiles = job.getInputFiles().stream()
                .map(filename -> state.file(filename).orElseThrow()
                        .getReferenceForPartitionId(job.getPartitionId()).orElseThrow())
                .toList();
        FileReference outputFile = job.getNewReference();
        if (outputFile.getNumberOfRows() != inputFiles.stream().mapToLong(FileReference::getNumberOfRows).sum()) {
            FileReference outputFileAfter = outputFile.toBuilder().lastStateStoreUpdateTime(entry.getUpdateTime()).build();
            return Optional.of(new CompactionChangedRowCount(job, inputFiles, outputFileAfter));
        } else {
            return Optional.empty();
        }
    }

    public String jobId() {
        return job.getJobId();
    }

    public String partitionId() {
        return job.getPartitionId();
    }

    public long rowsBefore() {
        return inputFiles.stream().mapToLong(FileReference::getNumberOfRows).sum();
    }

    public long rowsAfter() {
        return outputFile.getNumberOfRows();
    }

    public CompactionJob asCompactionJobToNewFile(String tableId, String outputFile) {
        return CompactionJob.builder()
                .tableId(tableId)
                .jobId(job.getJobId())
                .partitionId(job.getPartitionId())
                .inputFiles(job.getInputFiles())
                .outputFile(outputFile)
                .build();
    }
}
