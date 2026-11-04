/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.systemtest.dsl.compaction;

import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.compaction.core.job.commit.CompactionCommitMessageSerDe;

public record CompactionCommitMessageWrapper(CompactionCommitMessage message, String jobId, String body) {

    public static CompactionCommitMessageWrapper from(CompactionCommitMessage message) {
        return new CompactionCommitMessageWrapper(
                message,
                message.request().getJobId(),
                new CompactionCommitMessageSerDe().toJson(message));
    }

    public static CompactionCommitMessageWrapper fake(String name) {
        return new CompactionCommitMessageWrapper(null, name, name);
    }

}
