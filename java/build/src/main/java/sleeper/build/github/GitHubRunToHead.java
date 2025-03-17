/*
 * Copyright 2022-2025 Crown Copyright
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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GitHubRunToHead {

    private final GitHubWorkflowRun run;
    private final int aheadBy;
    private final int behindBy;
    private final List<String> changedPaths;

    public GitHubRunToHead(GitHubWorkflowRun run, int aheadBy, int behindBy, List<String> changedPaths) {
        this.run = Objects.requireNonNull(run, "run must not be null");
        this.aheadBy = aheadBy;
        this.behindBy = behindBy;
        this.changedPaths = Objects.requireNonNull(changedPaths, "changedPaths must not be null");
    }

    public static GitHubRunToHead sameSha(GitHubWorkflowRun run) {
        return new GitHubRunToHead(run, 0, 0, Collections.emptyList());
    }

    public GitHubWorkflowRun getRun() {
        return run;
    }

    public boolean isRunForHeadOrBehind() {
        return behindBy < 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GitHubRunToHead that = (GitHubRunToHead) o;
        return aheadBy == that.aheadBy && behindBy == that.behindBy && run.equals(that.run) && changedPaths.equals(that.changedPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(run, aheadBy, behindBy, changedPaths);
    }

    @Override
    public String toString() {
        return "GitHubRunToHead{" +
                "run=" + run +
                ", aheadBy=" + aheadBy +
                ", behindBy=" + behindBy +
                ", changedPaths=" + changedPaths +
                '}';
    }
}
