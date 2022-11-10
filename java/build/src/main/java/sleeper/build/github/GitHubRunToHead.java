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

import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;

import java.util.function.Function;

public class GitHubRunToHead {

    private final GitHubWorkflowRun run;
    private final boolean runForHeadOrBehind;

    public GitHubRunToHead(GitHubWorkflowRun run, boolean runForHeadOrBehind) {
        this.run = run;
        this.runForHeadOrBehind = runForHeadOrBehind;
    }

    public GitHubWorkflowRun getRun() {
        return run;
    }

    public boolean isRunForHeadOrBehind() {
        return runForHeadOrBehind;
    }

    public static GitHubRunToHead compare(
            WebTarget repository, Function<WebTarget, Invocation.Builder> request,
            GitHubWorkflowRun run, GitHubHead head) {
        if (run.isSameCommit(head)) {
            return new GitHubRunToHead(run, true);
        } else if (!run.isSameBranch(head)) {
            return new GitHubRunToHead(run, false);
        } else {
            WebTarget compare = repository.path("compare").path(run.getCommitSha() + "..." + head.getSha());
            GitHubCompareResponse response = request.apply(compare).buildGet().invoke(GitHubCompareResponse.class);
            return response.toRunToHead(run);
        }
    }
}
