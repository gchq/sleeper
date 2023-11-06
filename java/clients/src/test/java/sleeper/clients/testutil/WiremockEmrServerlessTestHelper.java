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

package sleeper.clients.testutil;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

public class WiremockEmrServerlessTestHelper {

    private WiremockEmrServerlessTestHelper() {
    }

    public static MappingBuilder listActiveEmrApplicationsRequest() {
        return get(listRunningApplicationsUrl());
    }

    public static MappingBuilder listRunningJobsForApplicationRequest() {
        return get(listRunningJobsUrl());
    }

    public static MappingBuilder listRunningOrCancellingJobsForApplicationRequest() {
        return get(listRunningOrCancellingJobsUrl());
    }

    public static MappingBuilder cancelJobRunRequest(String jobRunId) {
        return delete(cancelJobRunUrl(jobRunId));
    }

    public static MappingBuilder stopApplicationRequest() {
        return post(stopApplicationUrl());
    }

    public static RequestPatternBuilder anyRequestedForEmrServerless() {
        return anyRequestedFor(urlMatching("/applications.*"));
    }

    public static RequestPatternBuilder listActiveApplicationsRequested() {
        return getRequestedFor(listRunningApplicationsUrl());
    }

    public static RequestPatternBuilder listRunningJobsForApplicationRequested() {
        return getRequestedFor(listRunningJobsUrl());
    }

    public static RequestPatternBuilder listRunningOrCancellingJobsForApplicationRequested() {
        return getRequestedFor(listRunningOrCancellingJobsUrl());
    }

    public static RequestPatternBuilder stopApplicationRequested() {
        return postRequestedFor(stopApplicationUrl());
    }

    public static RequestPatternBuilder cancelJobRunRequested(String jobRunId) {
        return deleteRequestedFor(cancelJobRunUrl(jobRunId));
    }

    public static ResponseDefinitionBuilder aResponseWithNoApplications() {
        return aResponse().withStatus(200)
                .withBody("{\"applications\": []}");
    }

    public static ResponseDefinitionBuilder aResponseWithApplicationWithState(ApplicationState state) {
        return aResponseWithApplicationWithNameAndState("sleeper-test", state);
    }

    public static ResponseDefinitionBuilder aResponseWithApplicationWithNameAndState(String name, ApplicationState state) {
        return aResponse().withStatus(200)
                .withBody("{\"applications\": [{" +
                        "\"name\": \"" + name + "\"," +
                        "\"id\": \"test-app-id\"," +
                        "\"state\": \"" + state + "\"" +
                        "}]}");
    }

    public static ResponseDefinitionBuilder aResponseWithJobRunWithState(String jobRunId, JobRunState state) {
        return aResponse().withStatus(200).withBody("{\"jobRuns\":[{" +
                "\"applicationId\":\"test-app-id\"," +
                "\"id\":\"" + jobRunId + "\"," +
                "\"state\":\"" + state + "\"" +
                "}]}");
    }

    public static ResponseDefinitionBuilder aResponseWithNoJobRuns() {
        return aResponse().withStatus(200).withBody("{\"jobRuns\":[]}");
    }

    private static UrlPattern listRunningApplicationsUrl() {
        return urlEqualTo("/applications"
                + "?states=STARTING&states=STARTED&states=STOPPING");
    }

    private static UrlPattern listRunningJobsUrl() {
        return urlEqualTo("/applications/test-app-id/jobruns" +
                "?states=RUNNING&states=SCHEDULED&states=PENDING&states=SUBMITTED");
    }

    private static UrlPattern listRunningOrCancellingJobsUrl() {
        return urlEqualTo("/applications/test-app-id/jobruns" +
                "?states=RUNNING&states=SCHEDULED&states=PENDING&states=SUBMITTED&states=CANCELLING");
    }

    private static UrlPattern cancelJobRunUrl(String jobRunId) {
        return urlEqualTo("/applications/test-app-id/jobruns/" + jobRunId);
    }

    private static UrlPattern stopApplicationUrl() {
        return urlEqualTo("/applications/test-app-id/stop");
    }
}
