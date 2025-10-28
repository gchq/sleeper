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
     * @param  applicationId the application id
     * @return               a HTTP response
     */
    public static MappingBuilder getApplicationRequest(String applicationId) {
        return get(getApplicationUrl(applicationId));
    }

    /**
     * List running jobs on an EMR application.
     *
     * @param  applicationId the application id
     * @return               matching HTTP requests
     */
    public static MappingBuilder listRunningJobsForApplicationRequest(String applicationId) {
        return get(listRunningJobsUrl(applicationId));
    }

    /**
     * List running or cnacelled jobs on EMR application.
     *
     * @param  applicationId the application id
     * @return               matching HTTP requests
     */
    public static MappingBuilder listRunningOrCancellingJobsForApplicationRequest(String applicationId) {
        return get(listRunningOrCancellingJobsUrl(applicationId));
    }

    /**
     * Cancel a job run.
     *
     * @param  applicationId the application id
     * @param  jobRunId      the job id
     * @return               a HTTP response
     */
    public static MappingBuilder cancelJobRunRequest(String applicationId, String jobRunId) {
        return delete(cancelJobRunUrl(applicationId, jobRunId));
    }

    /**
     * Stop an EMR application.
     *
     * @param  applicationId the application id
     * @return               a HTTP response
     */
    public static MappingBuilder stopApplicationRequest(String applicationId) {
        return post(stopApplicationUrl(applicationId));
    }

    /**
     * Check for any requests to the EMR Serverless client.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder anyRequestedForEmrServerless() {
        return anyRequestedFor(urlMatching("/applications.*"));
    }

    /**
     * Get an EMR serverless application.
     *
     * @param  applicationId the application id
     * @return               the application
     */
    public static RequestPatternBuilder getApplicationRequested(String applicationId) {
        return getRequestedFor(getApplicationUrl(applicationId));
    }

    /**
     * Check for a list running jobs request.
     *
     * @param  applicationId the application id
     * @return               matching HTTP requests
     */
    public static RequestPatternBuilder listRunningJobsForApplicationRequested(String applicationId) {
        return getRequestedFor(listRunningJobsUrl(applicationId));
    }

    /**
     * Check for a list running or cancelled jobs request.
     *
     * @param  applicationId the application id
     * @return               matching HTTP requests
     */
    public static RequestPatternBuilder listRunningOrCancellingJobsForApplicationRequested(String applicationId) {
        return getRequestedFor(listRunningOrCancellingJobsUrl(applicationId));
    }

    /**
     * Check for a stop application request.
     *
     * @param  applicationId the application id
     * @return               matching HTTP requests
     */
    public static RequestPatternBuilder stopApplicationRequested(String applicationId) {
        return postRequestedFor(stopApplicationUrl(applicationId));
    }

    /**
     * Check for a job cancellation request.
     *
     * @param  applicationId the application id
     * @param  jobRunId      the job id
     * @return               matching HTTP requests
     */
    public static RequestPatternBuilder cancelJobRunRequested(String applicationId, String jobRunId) {
        return deleteRequestedFor(cancelJobRunUrl(applicationId, jobRunId));
    }

    /**
     * Build an EMR application response with a give job id and state.
     *
     * @param  applicationId the application id
     * @param  jobRunId      the job id
     * @param  state         the application state
     * @return               a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithJobRunWithState(String applicationId, String jobRunId, JobRunState state) {
        return aResponse().withStatus(200).withBody("{\"jobRuns\":[{" +
                "\"applicationId\":\"" + applicationId + "\"," +
                "\"id\":\"" + jobRunId + "\"," +
                "\"state\":\"" + state + "\"" +
                "}]}");
    }

    /**
     * Build an EMR application response for a terminated application.
     *
     * @param  applicationId the application id
     * @return               a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithTerminatedApplication(String applicationId) {
        return aResponse().withStatus(200).withBody("{\"application\":{" +
                "\"applicationId\":\"" + applicationId + "\"," +
                "\"state\":\"TERMINATED\"" +
                "}}");
    }

    /**
     * Build an EMR application response for a started application.
     *
     * @param  applicationId the application id
     * @return               a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithStartedApplication(String applicationId) {
        return aResponse().withStatus(200).withBody("{\"application\":{" +
                "\"applicationId\":\"" + applicationId + "\"," +
                "\"state\":\"STARTED\"" +
                "}}");
    }

    /**
     * Build an EMR application response for a stopping application.
     *
     * @param  applicationId the application id
     * @return               a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithStoppingApplication(String applicationId) {
        return aResponse().withStatus(200).withBody("{\"application\":{" +
                "\"applicationId\":\"" + applicationId + "\"," +
                "\"state\":\"STOPPING\"" +
                "}}");
    }

    /**
     * Build an EMR application response for a stopped application.
     *
     * @param  applicationId the application id
     * @return               a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithStoppedApplication(String applicationId) {
        return aResponse().withStatus(200).withBody("{\"application\":{" +
                "\"applicationId\":\"" + applicationId + "\"," +
                "\"state\":\"STOPPED\"" +
                "}}");
    }

    /**
     * Build an EMR application response with an empty list of job runs.
     *
     * @return a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithNoJobRuns() {
        return aResponse().withStatus(200).withBody("{\"jobRuns\":[]}");
    }

    private static UrlPattern getApplicationUrl(String applicationId) {
        return urlEqualTo("/applications/" + applicationId);
    }

    private static UrlPattern listRunningJobsUrl(String applicationId) {
        return urlEqualTo("/applications/" + applicationId + "/jobruns" +
                "?states=RUNNING&states=SCHEDULED&states=PENDING&states=SUBMITTED");
    }

    private static UrlPattern listRunningOrCancellingJobsUrl(String applicationId) {
        return urlEqualTo("/applications/" + applicationId + "/jobruns" +
                "?states=RUNNING&states=SCHEDULED&states=PENDING&states=SUBMITTED&states=CANCELLING");
    }

    private static UrlPattern cancelJobRunUrl(String applicationId, String jobRunId) {
        return urlEqualTo("/applications/" + applicationId + "/jobruns/" + jobRunId);
    }

    private static UrlPattern stopApplicationUrl(String applicationId) {
        return urlEqualTo("/applications/" + applicationId + "/stop");
    }

}
