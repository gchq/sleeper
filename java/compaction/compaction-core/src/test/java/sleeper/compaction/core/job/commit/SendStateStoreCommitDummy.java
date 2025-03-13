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
package sleeper.compaction.core.job.commit;

import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;

import java.util.Queue;

public class SendStateStoreCommitDummy implements StateStoreCommitRequestSender {

    private final Queue<StateStoreCommitRequest> queue;
    private final String failForTableId;

    private SendStateStoreCommitDummy(Queue<StateStoreCommitRequest> queue, String failForTableId) {
        this.queue = queue;
        this.failForTableId = failForTableId;
    }

    public static StateStoreCommitRequestSender sendToQueueExceptForTable(
            Queue<StateStoreCommitRequest> queue, String failForTableId) {
        return new SendStateStoreCommitDummy(queue, failForTableId);
    }

    @Override
    public void send(StateStoreCommitRequest request) {
        if (request.getTableId().equals(failForTableId)) {
            throw new RuntimeException("Dummy failure");
        } else {
            queue.add(request);
        }
    }

}
