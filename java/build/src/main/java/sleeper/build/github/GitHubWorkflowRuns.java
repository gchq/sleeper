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
package sleeper.build.github;

import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHWorkflowRun;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.PagedIterable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GitHubWorkflowRuns {

    private final GitHub gitHub;

    public GitHubWorkflowRuns(GitHub gitHub) {
        this.gitHub = gitHub;
    }

    public GitHubWorkflowRuns(String token) throws IOException {
        this.gitHub = new GitHubBuilder().withJwtToken(token).build();
    }

    public Optional<GitHubWorkflowRun> getLatestRun(GitHubHead head, String workflow) {
        return getRunsForHeadOrBehind(head, workflow).findFirst();
    }

    public Stream<GitHubWorkflowRun> getRunsForHeadOrBehind(GitHubHead head, String workflow) {
        try {
            GHRepository repository = repository(head);
            PagedIterable<GHWorkflowRun> iterable = repository.getWorkflow(workflow).listRuns();
            return StreamSupport.stream(iterable.spliterator(), false)
                    .map(run -> GitHubRunToHead.compare(repository, run, head))
                    .filter(GitHubRunToHead::isRunForHeadOrBehind)
                    .map(GitHubWorkflowRuns::convertToInternalRun);
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    public GitHubWorkflowRun recheckRun(GitHubHead head, Long runId) {
        try {
            return convertToInternalRun(repository(head).getWorkflowRun(runId));
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }

    private GHRepository repository(GitHubHead head) throws IOException {
        return gitHub.getRepository(head.getOwnerAndRepository());
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
