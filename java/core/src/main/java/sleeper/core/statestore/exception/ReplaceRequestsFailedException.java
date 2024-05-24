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
package sleeper.core.statestore.exception;

import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStoreException;

import java.util.List;

/**
 * An exception for when some file references could not be replaced. Some may have been successful, and this tracks
 * which succeeded and which failed.
 */
public class ReplaceRequestsFailedException extends StateStoreException {
    private final transient List<ReplaceFileReferencesRequest> successfulRequests;
    private final transient List<ReplaceFileReferencesRequest> failedRequests;
    private final transient List<Exception> failures;

    public ReplaceRequestsFailedException(List<ReplaceFileReferencesRequest> failedRequests, Exception failure) {
        this(List.of(), failedRequests, List.of(failure));
    }

    public ReplaceRequestsFailedException(
            List<ReplaceFileReferencesRequest> successfulRequests, List<ReplaceFileReferencesRequest> failedRequests, List<Exception> failures) {
        super(failedRequests.size() + " replace file reference requests failed to update the state store", failures.get(0));
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.failures = failures;
    }

    public List<ReplaceFileReferencesRequest> getSuccessfulRequests() {
        return successfulRequests;
    }

    public List<ReplaceFileReferencesRequest> getFailedRequests() {
        return failedRequests;
    }

    public List<Exception> getFailures() {
        return failures;
    }
}
