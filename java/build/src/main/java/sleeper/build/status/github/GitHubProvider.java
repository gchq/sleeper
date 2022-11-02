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
package sleeper.build.status.github;

import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHWorkflowRun;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterable;
import sleeper.build.status.ChunkStatus;
import sleeper.build.status.GitHubHead;
import sleeper.build.status.ProjectChunk;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class GitHubProvider {

    private final GitHub gitHub;

    public GitHubProvider(String token) throws IOException {
        this.gitHub = new GitHubBuilder().withJwtToken(token).build();
    }

    public ChunkStatus workflowStatus(GitHubHead head, ProjectChunk chunk) {
        try {
            GHRepository repository = repository(head);
            PagedIterable<GHWorkflowRun> iterable = repository.getWorkflow(chunk.getWorkflow()).listRuns();
            return StreamSupport.stream(iterable.spliterator(), false)
                    .map(run -> new GitHubRunToHead(repository, run, head))
                    .filter(GitHubRunToHead::isRunForHeadOrBehind)
                    .findFirst().map(run -> statusFrom(chunk, run.getRun()))
                    .orElseGet(() -> ChunkStatus.chunk(chunk).noBuild());
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    public ChunkStatus recheckRun(GitHubHead head, ChunkStatus status) {
        try {
            return statusFrom(status.getChunk(), repository(head).getWorkflowRun(status.getRunId()));
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private static ChunkStatus statusFrom(ProjectChunk chunk, GHWorkflowRun run) {
        try {
            return ChunkStatus.chunk(chunk)
                    .runId(run.getId())
                    .runUrl(Objects.toString(run.getHtmlUrl(), null))
                    .runStarted(run.getRunStartedAt())
                    .commitSha(run.getHeadSha())
                    .commitMessage(run.getHeadCommit().getMessage())
                    .status(Objects.toString(run.getStatus(), null))
                    .conclusion(Objects.toString(run.getConclusion(), null))
                    .build();
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private GHRepository repository(GitHubHead head) throws IOException {
        return gitHub.getRepository(head.getOwnerAndRepository());
    }

}
