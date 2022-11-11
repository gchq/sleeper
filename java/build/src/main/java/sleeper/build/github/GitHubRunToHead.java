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

import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHCompare;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHWorkflowRun;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GitHubRunToHead {

    private final GHCompare compare;
    private final GHWorkflowRun run;
    private final GitHubHead head;

    private GitHubRunToHead(GHCompare compare, GHWorkflowRun run, GitHubHead head) {
        this.compare = compare;
        this.run = run;
        this.head = head;
    }

    public GHWorkflowRun getRun() {
        return run;
    }

    public boolean isRunForHeadOrBehind() {
        // Ignore builds for commits that are not in the history of the head commit.
        // If a rebase or other forced push leaves a build that is disconnected from the new head, it should be ignored.
        return head.getBranch().equals(run.getHeadBranch())
                && (head.getSha().equals(run.getHeadSha()) || numCommitsHeadBehindRun() < 1);
    }

    private int numCommitsHeadBehindRun() {
        return compare.getBehindBy();
    }

    public List<String> buildChangedPaths() {
        return Arrays.stream(compare.getFiles())
                .map(GHCommit.File::getFileName)
                .collect(Collectors.toList());
    }

    public static GitHubRunToHead compare(GHRepository repository, GHWorkflowRun run, GitHubHead head) {
        try {
            return new GitHubRunToHead(repository.getCompare(run.getHeadSha(), head.getSha()), run, head);
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }
}
