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
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;

import java.io.IOException;

public class GitHubProvider {

    private final GitHub gitHub;

    public GitHubProvider(String token) throws IOException {
        this.gitHub = new GitHubBuilder().withJwtToken(token).build();
    }

    public ChunkStatus workflowStatus(GitHubBranch branch, ProjectChunk chunk) {
        // TODO
        try {
            repository(branch).getWorkflow(chunk.getWorkflow()).listRuns();
            repository(branch).queryWorkflowRuns().branch(branch.getBranch());
        } catch (IOException e) {
            throw new GitHubException(e);
        }
        return null;
    }

    private GHRepository repository(GitHubBranch branch) throws IOException {
        return gitHub.getRepository(branch.getOwnerAndRepository());
    }

}
