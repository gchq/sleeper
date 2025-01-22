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

import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;

/**
 * A request to include a single compaction job commit in a batch update. Used in the compaction commit batcher, and
 * combined into a single {@link StateStoreCommitRequest} per Sleeper table, in a
 * {@link ReplaceFileReferencesTransaction}.
 *
 * @param tableId        the Sleeper table ID
 * @param request        details of the compaction commit, to be included in a transaction
 * @param callbackOnFail a callback to be run if the commit fails
 */
public record CompactionCommitRequest(String tableId, ReplaceFileReferencesRequest request, Runnable callbackOnFail) {
}
