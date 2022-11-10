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

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;

import java.util.stream.Stream;

public class GitHubWorkflowRunsImpl implements GitHubWorkflowRuns, AutoCloseable {

    private final String token;
    private final Client client;
    private final WebTarget gitHubApi;

    public GitHubWorkflowRunsImpl(String token) {
        this.token = token;
        client = ClientBuilder.newClient();
        gitHubApi = client.target("https://api.github.com");
    }

    @Override
    public Stream<GitHubWorkflowRun> getRunsForHeadOrBehindLatestFirst(GitHubHead head, String workflow) {
        WebTarget repository = repository(head);
        WebTarget runs = repository.path("actions/workflows").path(workflow)
                .path("runs").queryParam("branch", head.getBranch());
        GitHubWorkflowRunsResponse response = request(runs).buildGet()
                .invoke(GitHubWorkflowRunsResponse.class);
        return response.getWorkflowRuns().stream()
                .map(GitHubWorkflowRunsResponse.Run::toInternalRun)
                .map(run -> GitHubRunToHead.compare(repository, this::request, run, head))
                .filter(GitHubRunToHead::isRunForHeadOrBehind)
                .map(GitHubRunToHead::getRun);
    }

    @Override
    public GitHubWorkflowRun recheckRun(GitHubHead head, Long runId) {
        WebTarget repository = repository(head);
        WebTarget run = repository.path("actions/runs").path("" + runId);
        GitHubWorkflowRunsResponse.Run response = request(run).buildGet()
                .invoke(GitHubWorkflowRunsResponse.Run.class);
        return response.toInternalRun();
    }

    private WebTarget repository(GitHubHead head) {
        return gitHubApi.path("repos").path(head.getOwnerAndRepository());
    }

    private Invocation.Builder request(WebTarget target) {
        return target.request("application/vnd.github+json")
                .header("Authorization", "Bearer " + token);
    }

    @Override
    public void close() {
        client.close();
    }
}
