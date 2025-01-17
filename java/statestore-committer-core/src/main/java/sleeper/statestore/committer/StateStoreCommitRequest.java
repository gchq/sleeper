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
package sleeper.statestore.committer;

import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransaction;

import java.util.Objects;

/**
 * A request to commit updates to the state store.
 */
public class StateStoreCommitRequest {

    private final Object request;
    private final String tableId;
    private final ApplyRequest applyRequest;

    /**
     * Creates a request to commit a transaction.
     *
     * @param  request the request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forTransaction(StateStoreCommitRequestByTransaction request) {
        return new StateStoreCommitRequest(request, request.getTableId(), committer -> committer.addTransaction(request));
    }

    private StateStoreCommitRequest(Object request, String tableId, ApplyRequest applyRequest) {
        this.request = request;
        this.tableId = tableId;
        this.applyRequest = applyRequest;
    }

    public Object getRequest() {
        return request;
    }

    public String getTableId() {
        return tableId;
    }

    void apply(StateStoreCommitter committer) {
        applyRequest.apply(committer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(request);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitRequest)) {
            return false;
        }
        StateStoreCommitRequest other = (StateStoreCommitRequest) obj;
        return Objects.equals(request, other.request);
    }

    @Override
    public String toString() {
        return "StateStoreCommitRequest{request=" + request + "}";
    }

    /**
     * Applies the current request with a given committer.
     */
    @FunctionalInterface
    private interface ApplyRequest {
        void apply(StateStoreCommitter committer) throws StateStoreException;
    }
}
