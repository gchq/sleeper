/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.build.github.api;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import sleeper.build.github.GitHubHead;
import sleeper.build.github.GitHubWorkflowRun;
import sleeper.build.github.TestGitHubHead;

import java.time.Instant;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubRequest;
import static sleeper.build.github.api.GitHubApiTestHelper.gitHubResponse;
import static sleeper.build.github.api.GitHubApiTestHelper.returnWithGitHubApi;
import static sleeper.build.testutil.TestResources.exampleString;

@WireMockTest
class GitHubWorkflowRunsImplIT {

    private static final GitHubHead GITHUB_EXAMPLE_HEAD = TestGitHubHead.exampleBuilder()
            .sha("acb5820ced9479c074f688cc328bf03f341a511d").build();


    private Optional<GitHubWorkflowRun> getLatestRun(
            WireMockRuntimeInfo runtimeInfo, GitHubHead head, String workflow) {
        return returnWithGitHubApi(runtimeInfo, api -> new GitHubWorkflowRunsImpl(api).getLatestRun(head, workflow));
    }

    @Test
    void shouldGetSingleWorkflowRun(WireMockRuntimeInfo runtimeInfo) {
        stubFor(gitHubRequest(get("/repos/test-owner/test-repo/actions/workflows/test-workflow.yaml/runs?branch=test-branch"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/workflow-runs-single.json"))));

        assertThat(getLatestRun(runtimeInfo, GITHUB_EXAMPLE_HEAD, "test-workflow.yaml"))
                .contains(GitHubWorkflowRun.builder()
                        .status("queued").runId(30433642L)
                        .runUrl("https://github.com/octo-org/octo-repo/actions/runs/30433642")
                        .runStarted(Instant.parse("2020-01-22T19:33:08Z"))
                        .commitSha(GITHUB_EXAMPLE_HEAD.getSha())
                        .commitMessage("Create linter.yaml")
                        .build());
    }

    @Test
    void shouldCompareOldCommitWithThisCommit(WireMockRuntimeInfo runtimeInfo) {
        stubFor(gitHubRequest(get("/repos/test-owner/test-repo/actions/workflows/test-workflow.yaml/runs?branch=test-branch"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/workflow-runs-single.json"))));

        stubFor(gitHubRequest(get("/repos/test-owner/test-repo/compare/" + GITHUB_EXAMPLE_HEAD.getSha() + "...test-sha"))
                .willReturn(gitHubResponse()
                        .withStatus(200)
                        .withBody(exampleString("examples/github-api/compare.json"))));

        assertThat(getLatestRun(runtimeInfo, TestGitHubHead.example(), "test-workflow.yaml"))
                .contains(GitHubWorkflowRun.builder()
                        .status("queued").runId(30433642L)
                        .runUrl("https://github.com/octo-org/octo-repo/actions/runs/30433642")
                        .runStarted(Instant.parse("2020-01-22T19:33:08Z"))
                        .commitSha(GITHUB_EXAMPLE_HEAD.getSha())
                        .commitMessage("Create linter.yaml")
                        .build());
    }
}
