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
package sleeper.cdk.custom;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
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
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

/**
 * Helper methods to mock an EMR Serverless application.
 */
public class WiremockEmrServerlessTestHelper {

    private WiremockEmrServerlessTestHelper() {
    }

    /**
     * Creates a mocked EMR Servless client.
     *
     * @param  runtimeInfo wire mocks runtime info
     * @return             the EMR Serverless client
     */
    public static EmrServerlessClient wiremockEmrServerlessClient(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, EmrServerlessClient.builder());
    }

    /**
     * Get an EMR application.
     *
     * @return a HTTP response
     */
    public static MappingBuilder getApplicationRequest() {
        return get(getApplicationUrl());
    }

    /**
     * List running jobs on an EMR application.
     *
     * @return matching HTTP requests
     */
    public static MappingBuilder listRunningJobsForApplicationRequest() {
        return get(listRunningJobsUrl());
    }

    /**
     * List running or cnacelled jobs on EMR application.
     *
     * @return matching HTTP requests
     */
    public static MappingBuilder listRunningOrCancellingJobsForApplicationRequest() {
        return get(listRunningOrCancellingJobsUrl());
    }

    /**
     * Cancel a job run.
     *
     * @param  jobRunId the job id
     * @return          a HTTP response
     */
    public static MappingBuilder cancelJobRunRequest(String jobRunId) {
        return delete(cancelJobRunUrl(jobRunId));
    }

    /**
     * Stop an EMR application.
     *
     * @return a HTTP response
     */
    public static MappingBuilder stopApplicationRequest() {
        return post(stopApplicationUrl());
    }

    /**
     * Check for any requests to the EMR Serverless client.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder anyRequestedForEmrServerless() {
        return anyRequestedFor(urlMatching("/applications.*"));
    }

    public static RequestPatternBuilder getApplicationRequested() {
        return getRequestedFor(getApplicationUrl());
    }

    /**
     * Check for a list running jobs request.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder listRunningJobsForApplicationRequested() {
        return getRequestedFor(listRunningJobsUrl());
    }

    /**
     * Check for a list running or cancelled jobs request.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder listRunningOrCancellingJobsForApplicationRequested() {
        return getRequestedFor(listRunningOrCancellingJobsUrl());
    }

    /**
     * Check for a stop application request.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder stopApplicationRequested() {
        return postRequestedFor(stopApplicationUrl());
    }

    /**
     * Check for a job cancellation request.
     *
     * @param  jobRunId the job id
     * @return          matching HTTP requests
     */
    public static RequestPatternBuilder cancelJobRunRequested(String jobRunId) {
        return deleteRequestedFor(cancelJobRunUrl(jobRunId));
    }

    /**
     * Build an EMR application response with a give job id and state.
     *
     * @param  jobRunId the job id
     * @param  state    the application state
     * @return          a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithJobRunWithState(String jobRunId, JobRunState state) {
        return aResponse().withStatus(200).withBody("{\"jobRuns\":[{" +
                "\"applicationId\":\"test-app-id\"," +
                "\"id\":\"" + jobRunId + "\"," +
                "\"state\":\"" + state + "\"" +
                "}]}");
    }

    public static ResponseDefinitionBuilder aResponseWithNoApplication() {
        return aResponse().withStatus(404)
                .withHeader("X-Amzn-ErrorType", "ResourceNotFoundException");
    }

    /**
     * Build an EMR application response with an empty list of job runs.
     *
     * @return a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithNoJobRuns() {
        return aResponse().withStatus(200).withBody("{\"jobRuns\":[]}");
    }

    private static UrlPattern getApplicationUrl() {
        return urlEqualTo("/applications/test-app-id");
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
