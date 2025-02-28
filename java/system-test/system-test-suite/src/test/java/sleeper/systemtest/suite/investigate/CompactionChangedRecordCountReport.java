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
package sleeper.systemtest.suite.investigate;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;

import java.util.ArrayList;
import java.util.List;

public record CompactionChangedRecordCountReport(TransactionLogEntry entry, ReplaceFileReferencesTransaction transaction, List<CompactionChangedRecordCount> jobs) {

    public static List<CompactionChangedRecordCountReport> findChanges(List<TransactionLogEntryHandle> filesLog) {
        StateStoreFiles state = new StateStoreFiles();
        List<CompactionChangedRecordCountReport> reports = new ArrayList<>();
        for (TransactionLogEntryHandle entry : filesLog) {
            if (entry.isType(TransactionType.REPLACE_FILE_REFERENCES)) {
                ReplaceFileReferencesTransaction transaction = entry.castTransaction();
                List<CompactionChangedRecordCount> changes = new ArrayList<>();
                for (ReplaceFileReferencesRequest job : transaction.getJobs()) {
                    String partitionId = job.getPartitionId();
                    List<FileReference> inputFiles = job.getInputFiles().stream()
                            .map(filename -> state.file(filename).orElseThrow()
                                    .getReferenceForPartitionId(partitionId).orElseThrow())
                            .toList();
                    FileReference outputFile = job.getNewReference();
                    if (outputFile.getNumberOfRecords() != inputFiles.stream().mapToLong(FileReference::getNumberOfRecords).sum()) {
                        FileReference outputFileAfter = outputFile.toBuilder().lastStateStoreUpdateTime(entry.original().getUpdateTime()).build();
                        changes.add(new CompactionChangedRecordCount(job, inputFiles, outputFileAfter));
                    }
                }
                if (!changes.isEmpty()) {
                    reports.add(new CompactionChangedRecordCountReport(entry.original(), entry.castTransaction(), changes));
                }
            }
            entry.apply(state);
        }
        return reports;
    }

    public long transactionNumber() {
        return entry.getTransactionNumber();
    }
}
