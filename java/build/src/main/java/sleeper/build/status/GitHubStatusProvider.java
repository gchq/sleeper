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

import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHWorkflowRun;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterable;
import sleeper.build.chunks.ProjectChunk;
import sleeper.build.github.GitHubException;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubRunToHead;
import sleeper.build.github.GitHubWorkflowRun;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class GitHubStatusProvider {

    private final GitHub gitHub;

    public GitHubStatusProvider(String token) throws IOException {
        this(new GitHubBuilder().withJwtToken(token).build());
    }

    public GitHubStatusProvider(GitHub gitHub) {
        this.gitHub = gitHub;
    }

    public ChunkStatus workflowStatus(GitHubHead head, ProjectChunk chunk) {
        return getLatestRun(head, chunk.getWorkflow())
                .map(run -> statusFrom(chunk, run))
                .orElseGet(() -> ChunkStatus.chunk(chunk).noBuild());
    }

    public ChunkStatus recheckRun(GitHubHead head, ChunkStatus status) {
        return statusFrom(status, recheckRun(head, status.getRunId()));
    }

    public GitHubWorkflowRun recheckRun(GitHubHead head, Long runId) {
        try {
            return convertToInternalRun(repository(head).getWorkflowRun(runId));
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private static ChunkStatus statusFrom(ProjectChunk chunk, GitHubWorkflowRun run) {
        return ChunkStatus.chunk(chunk).run(run).build();
    }

    private static ChunkStatus statusFrom(ChunkStatus status, GitHubWorkflowRun run) {
        return ChunkStatus.chunk(status.getChunk()).run(run).build();
    }

    private GHRepository repository(GitHubHead head) throws IOException {
        return gitHub.getRepository(head.getOwnerAndRepository());
    }

    public Optional<GitHubWorkflowRun> getLatestRun(GitHubHead head, String workflow) {
        try {
            GHRepository repository = repository(head);
            PagedIterable<GHWorkflowRun> iterable = repository.getWorkflow(workflow).listRuns();
            return StreamSupport.stream(iterable.spliterator(), false)
                    .map(run -> GitHubRunToHead.compare(repository, run, head))
                    .filter(GitHubRunToHead::isRunForHeadOrBehind)
                    .findFirst().map(GitHubStatusProvider::convertToInternalRun);
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private static GitHubWorkflowRun convertToInternalRun(GitHubRunToHead run) {
        return convertToInternalRun(run.getRun());
    }

    private static GitHubWorkflowRun convertToInternalRun(GHWorkflowRun run) {
        try {
            return GitHubWorkflowRun.builder().runId(run.getId())
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
}
