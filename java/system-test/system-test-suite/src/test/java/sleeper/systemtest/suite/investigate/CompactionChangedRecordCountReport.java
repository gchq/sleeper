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

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;

import java.util.List;
import java.util.Optional;

public record CompactionChangedRecordCountReport(TransactionLogEntry entry, ReplaceFileReferencesTransaction transaction, List<CompactionChangedRecordCount> jobs) {

    public static List<CompactionChangedRecordCountReport> findChanges(List<TransactionLogEntryHandle> filesLog) {
        StateStoreFiles state = new StateStoreFiles();
        List<CompactionChangedRecordCountReport> reports = filesLog.stream()
                .flatMap(entry -> {
                    Optional<CompactionChangedRecordCountReport> change = findChange(entry, state);
                    entry.apply(state);
                    return change.stream();
                })
                .toList();
        return reports;
    }

    private static Optional<CompactionChangedRecordCountReport> findChange(TransactionLogEntryHandle entry, StateStoreFiles state) {
        if (!entry.isType(TransactionType.REPLACE_FILE_REFERENCES)) {
            return Optional.empty();
        }
        ReplaceFileReferencesTransaction transaction = entry.castTransaction();
        List<CompactionChangedRecordCount> changes = CompactionChangedRecordCount.detectChanges(transaction, entry, state);
        if (!changes.isEmpty()) {
            return Optional.of(new CompactionChangedRecordCountReport(entry.original(), entry.castTransaction(), changes));
        } else {
            return Optional.empty();
        }
    }

    public long transactionNumber() {
        return entry.getTransactionNumber();
    }
}
