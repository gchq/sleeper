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

import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import sleeper.build.chunks.ProjectChunk;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.GitHubWorkflowRuns;

import java.io.IOException;

public class GitHubStatusProvider {

    private final GitHubWorkflowRuns runs;

    public GitHubStatusProvider(String token) throws IOException {
        this(new GitHubBuilder().withJwtToken(token).build());
    }

    public GitHubStatusProvider(GitHub gitHub) {
        this.runs = new GitHubWorkflowRuns(gitHub);
    }

    public ChunkStatus workflowStatus(GitHubHead head, ProjectChunk chunk) {
        return runs.getLatestRun(head, chunk.getWorkflow())
                .map(run -> statusFrom(chunk, run))
                .orElseGet(() -> ChunkStatus.chunk(chunk).noBuild());
    }

    public ChunkStatus recheckRun(GitHubHead head, ChunkStatus status) {
        return statusFrom(status, runs.recheckRun(head, status.getRunId()));
    }

    private static ChunkStatus statusFrom(ProjectChunk chunk, GitHubWorkflowRun run) {
        return ChunkStatus.chunk(chunk).run(run).build();
    }

    private static ChunkStatus statusFrom(ChunkStatus status, GitHubWorkflowRun run) {
        return ChunkStatus.chunk(status.getChunk()).run(run).build();
    }

}
