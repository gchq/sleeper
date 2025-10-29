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
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ClusterState;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

public class WiremockEmrNonPersistentTestHelper {
    private WiremockEmrNonPersistentTestHelper() {
    }

    public static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_DESCRIBE_CLUSTER_OPERATION = equalTo("ElasticMapReduce.DescribeCluster");
    private static final StringValuePattern MATCHING_TERMINATE_JOB_FLOWS_OPERATION = equalTo("ElasticMapReduce.TerminateJobFlows");

    /**
     * Creates a mocked EMR Servless client.
     *
     * @param  runtimeInfo wire mocks runtime info
     * @return             the EMR Serverless client
     */
    public static EmrClient wiremockEmrClient(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, EmrClient.builder());
    }

    public static RequestPatternBuilder anyRequestedForEmr() {
        return anyRequestedFor(anyUrl())
                .withHeader(OPERATION_HEADER, matching("^ElasticMapReduce\\..*"));
    }

    public static ResponseDefinitionBuilder aResponseWithNoClusters() {
        return aResponse().withStatus(200).withBody("{\"Clusters\": []}");
    }

    /**
     * Build an EMR application response with a give job id and state.
     *
     * @param  clusterId the cluster id
     * @param  state     the cluster state
     * @return           a HTTP response
     */
    public static ResponseDefinitionBuilder aResponseWithClusterWithState(String clusterId, ClusterState state) {
        return aResponse().withStatus(200).withBody("{\"Cluster\": {" +
                "\"Id\":\"" + clusterId + "\"," +
                "\"Name\":\"sleeper-test-instance-test-cluster\"," +
                "\"Status\": {\"State\": \"" + state.toString() + "\"}" +
                "}}");
    }

    public static MappingBuilder describeClusterRequest(String clusterId) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DESCRIBE_CLUSTER_OPERATION)
                .willReturn(aResponse().withStatus(200));
    }

    public static MappingBuilder terminateJobFlowsRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION)
                .willReturn(aResponse().withStatus(200));
    }

    public static RequestPatternBuilder terminateJobFlowsRequestedFor(String clusterId) {
        return terminateJobFlowsRequested()
                .withRequestBody(matchingJsonPath("$.JobFlowIds",
                        equalTo(clusterId)));
    }

    public static RequestPatternBuilder terminateJobFlowsRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION);
    }

    public static RequestPatternBuilder describeClusterRequestedFor(String clusterId) {
        return describeClusterRequested()
                .withRequestBody(matchingJsonPath("$.ClusterId",
                        equalTo(clusterId)));
    }

    public static RequestPatternBuilder describeClusterRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DESCRIBE_CLUSTER_OPERATION);
    }

}
