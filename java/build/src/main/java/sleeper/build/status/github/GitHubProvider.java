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
import sleeper.build.status.GitHubBranch;
import sleeper.build.status.ProjectChunk;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class GitHubProvider {

    private final GitHub gitHub;

    public GitHubProvider(String token) throws IOException {
        this.gitHub = new GitHubBuilder().withJwtToken(token).build();
    }

    public ChunkStatus workflowStatus(GitHubBranch branch, ProjectChunk chunk) {
        try {
            PagedIterable<GHWorkflowRun> iterable = repository(branch).getWorkflow(chunk.getWorkflow()).listRuns();
            return StreamSupport.stream(iterable.spliterator(), false)
                    .filter(run -> branch.getBranch().equals(run.getHeadBranch()))
                    .findFirst().map(run -> statusFrom(chunk, run))
                    .orElseGet(() -> ChunkStatus.chunk(chunk).noBuild());
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private static ChunkStatus statusFrom(ProjectChunk chunk, GHWorkflowRun run) {
        try {
            Date time = run.getRunStartedAt();
            return ChunkStatus.chunk(chunk)
                    .runUrl(Objects.toString(run.getHtmlUrl(), null))
                    .commitSha(run.getHeadSha())
                    .commitMessage(run.getHeadCommit().getMessage())
                    .status(Objects.toString(run.getStatus(), null))
                    .conclusion(Objects.toString(run.getConclusion(), null))
                    .build();
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private GHRepository repository(GitHubBranch branch) throws IOException {
        return gitHub.getRepository(branch.getOwnerAndRepository());
    }

}
