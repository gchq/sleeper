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

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

public class WiremockEmrServerlessTestHelper {

    public static final String OPERATION_HEADER = "X-Amz-Target";
    public static final StringValuePattern MATCHING_LIST_CONTAINERS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListContainerInstances");
    public static final StringValuePattern MATCHING_LIST_APPLICATIONS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListApplications");
    public static final StringValuePattern MATCHING_STOP_APPLICATION_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.StopApplication");
    public static final StringValuePattern MATCHING_CREATE_APPLICATION_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.CreateApplication");
    public static final StringValuePattern MATCHING_UPDATE_APPLICATION_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.UpdateApplication");
    public static final StringValuePattern MATCHING_DEREGISTER_CONTAINER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeregisterContainerInstance");
    public static final StringValuePattern MATCHING_DELETE_CLUSTER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeleteCluster");
    public static final StringValuePattern MATCHING_TAG_RESOURCE_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.TagResource");

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
     * Checks for an EMR Serverless stop application request.
     *
     * @param  applicationId the EMR serverless application id
     * @return               matching HTTP requests
     */
    public static RequestPatternBuilder stopApplicationRequestedFor(String applicationId) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_STOP_APPLICATION_OPERATION)
                .withRequestBody(matchingJsonPath("$.applicationId", equalTo(applicationId)));
    }

    /**
     * Checks for an EMR Serverless application create request.
     *
     * @param  applicationName the EMR application name
     * @return                 matching HTTP requests
     */
    public static RequestPatternBuilder createApplicationRequestedFor(String applicationName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_CREATE_APPLICATION_OPERATION)
                .withRequestBody(matchingJsonPath("$.name", equalTo(applicationName)));
    }

    /**
     * Checks for an EMR Servelss application update request.
     *
     * @param  applicationId the EMR Serverless application id
     * @return               matching HTTP requests
     */
    public static RequestPatternBuilder updateApplicationRequestedFor(String applicationId) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_UPDATE_APPLICATION_OPERATION)
                .withRequestBody(matchingJsonPath("$.applicationId", equalTo(applicationId)));
    }

    /**
     * Checks for any request that matches the pattern.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder anyRequestedForEmrServerless() {
        return anyRequestedFor(anyUrl())
                .withHeader(OPERATION_HEADER, matching("^AmazonEC2ContainerServiceV\\d+\\..*"));
    }

}
