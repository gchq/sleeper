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
package sleeper.compaction.core.job.commit;

import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

public class CompactionCommitBatcher {

    private final SendStateStoreCommit sendStateStoreCommit;

    public CompactionCommitBatcher(SendStateStoreCommit sendStateStoreCommit) {
        this.sendStateStoreCommit = sendStateStoreCommit;
    }

    public void sendBatch(List<CompactionCommitRequest> requests) {
        Map<String, List<CompactionCommitRequest>> requestsByTableId = requests.stream()
                .collect(groupingBy(CompactionCommitRequest::tableId));
        requestsByTableId.forEach(this::sendTransaction);
    }

    private void sendTransaction(String tableId, List<CompactionCommitRequest> requests) {
        ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(
                requests.stream().map(CompactionCommitRequest::request).toList());
        sendStateStoreCommit.send(StateStoreCommitRequest.create(tableId, transaction));
    }

    public interface SendStateStoreCommit {
        void send(StateStoreCommitRequest request);
    }

}
