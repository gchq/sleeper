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

package sleeper.core.statestore;

import java.util.List;

public class SplitRequestsFailedException extends StateStoreException {
    private final transient List<SplitFileReferenceRequest> successfulRequests;
    private final transient List<SplitFileReferenceRequest> failedRequests;

    public SplitRequestsFailedException(
            List<SplitFileReferenceRequest> successfulRequests, List<SplitFileReferenceRequest> failedRequests, Throwable cause) {
        super(failedRequests.size() + " split requests failed to update the state store", cause);
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
    }

    public SplitRequestsFailedException(
            String message, List<SplitFileReferenceRequest> successfulRequests, List<SplitFileReferenceRequest> failedRequests) {
        super(message);
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
    }

    public List<SplitFileReferenceRequest> getSuccessfulRequests() {
        return successfulRequests;
    }

    public List<SplitFileReferenceRequest> getFailedRequests() {
        return failedRequests;
    }
}
