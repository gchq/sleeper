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

import sleeper.core.statestore.ReplaceFileReferencesRequest;

/**
 * A request to include a single compaction job commit in a batch update. This will be picked up by the batcher as
 * a {@link CompactionCommitMessageHandle}.
 *
 * @param tableId the Sleeper table ID
 * @param request details of the compaction commit, to be included in a transaction
 */
public record CompactionCommitMessage(String tableId, ReplaceFileReferencesRequest request) {
}
