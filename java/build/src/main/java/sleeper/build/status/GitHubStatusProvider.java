/*
 * Copyright 2022 Crown Copyright
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
package sleeper.build.status;

import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.GitHubWorkflowRuns;

public class GitHubStatusProvider {

    private final GitHubWorkflowRuns runs;

    public GitHubStatusProvider(GitHubWorkflowRuns runs) {
        this.runs = runs;
    }

    public ChunkStatus recheckRun(GitHubHead head, ChunkStatus status) {
        return statusFrom(status, runs.recheckRun(head, status.getRunId()));
    }

    private static ChunkStatus statusFrom(ChunkStatus status, GitHubWorkflowRun run) {
        return ChunkStatus.chunk(status.getChunk()).run(run).build();
    }
}
