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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

/**
 * Combines multiple compaction commits into a single transaction per Sleeper table.
 */
public class CompactionCommitBatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionCommitBatcher.class);

    private final StateStoreCommitRequestSender sendStateStoreCommit;

    public CompactionCommitBatcher(StateStoreCommitRequestSender sendStateStoreCommit) {
        this.sendStateStoreCommit = sendStateStoreCommit;
    }

    public void sendBatch(List<CompactionCommitRequest> requests) {
        Map<String, List<CompactionCommitRequest>> requestsByTableId = requests.stream()
                .collect(groupingBy(CompactionCommitRequest::tableId));
        requestsByTableId.forEach(this::sendTransaction);
    }

    private void sendTransaction(String tableId, List<CompactionCommitRequest> requests) {
        try {
            ReplaceFileReferencesTransaction transaction = new ReplaceFileReferencesTransaction(
                    requests.stream().map(CompactionCommitRequest::request).toList());
            sendStateStoreCommit.send(StateStoreCommitRequest.create(tableId, transaction));
        } catch (Exception ex) {
            LOGGER.error("Failed to send message to state store commit queue with {} compactions for table ID {}", requests.size(), tableId, ex);
            requests.forEach(request -> request.callbackOnFail().run());
        }
    }

}
