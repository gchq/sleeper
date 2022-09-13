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
import sleeper.build.status.GitHubHead;

import java.io.IOException;

public class GitHubRunToHead {

    private final GHRepository repository;
    private final GHWorkflowRun run;
    private final GitHubHead head;

    public GitHubRunToHead(GHRepository repository, GHWorkflowRun run, GitHubHead head) {
        this.repository = repository;
        this.run = run;
        this.head = head;
    }

    public GHWorkflowRun getRun() {
        return run;
    }

    public boolean isRunForHeadOrBehind() {
        return head.getBranch().equals(run.getHeadBranch())
                && (head.getSha().equals(run.getHeadSha()) || numCommitsHeadBehindRun() < 1);
    }

    private int numCommitsHeadBehindRun() {
        try {
            return repository.getCompare(run.getHeadSha(), head.getSha()).getBehindBy();
        } catch (IOException e) {
            throw new GitHubException(e);
        }
    }
}
