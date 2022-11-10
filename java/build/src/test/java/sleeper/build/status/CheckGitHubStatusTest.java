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

import org.junit.Test;
import sleeper.build.chunks.ProjectChunks;
import sleeper.build.chunks.ProjectConfiguration;
import sleeper.build.chunks.TestChunks;
import sleeper.build.github.GitHubHead;
import sleeper.build.github.InMemoryGitHubWorkflowRuns;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckGitHubStatusTest {

    private static final ProjectChunks CHUNKS = TestChunks.example("example-chunks.yaml");
    private static final GitHubHead BRANCH = GitHubHead.builder()
            .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
            .build();
    private static final String WORKFLOW = "test-workflow.yaml";
    private final InMemoryGitHubWorkflowRuns workflowRuns = new InMemoryGitHubWorkflowRuns(BRANCH, WORKFLOW);

    private ProjectConfiguration.Builder configurationBuilder() {
        return ProjectConfiguration.builder()
                .token("test-token").head(BRANCH)
                .chunks(CHUNKS);
    }

    @Test
    public void shouldBuildAllChunksWhenNoWorkflowRunsYet() throws Exception {

        WorkflowStatus status = configurationBuilder().build().checkStatusSingleWorkflow(workflowRuns, WORKFLOW);
        assertThat(status.hasPreviousFailures()).isFalse();
        assertThat(status.previousBuildsReportLines()).containsExactly("",
                "Bulk Import: null",
                "",
                "Common: null",
                "",
                "Ingest: null");
        assertThat(status.chunkIdsToBuild()).containsExactly("bulk-import", "common", "ingest");
    }

}
