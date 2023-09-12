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
import com.github.tomakehurst.wiremock.matching.StringValuePattern;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static sleeper.clients.testutil.ClientWiremockTestHelper.OPERATION_HEADER;


public class WiremockEmrTestHelper {
    private WiremockEmrTestHelper() {
    }

    private static final StringValuePattern MATCHING_LIST_CLUSTERS_OPERATION = matching("ElasticMapReduce.ListClusters");
    private static final StringValuePattern MATCHING_LIST_STEPS_OPERATION = matching("ElasticMapReduce.ListSteps");
    private static final StringValuePattern MATCHING_TERMINATE_JOB_FLOWS_OPERATION = matching("ElasticMapReduce.TerminateJobFlows");

    public static MappingBuilder listActiveClustersRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_CLUSTERS_OPERATION)
                .withRequestBody(equalToJson("{\"ClusterStates\":[" +
                        "\"STARTING\",\"BOOTSTRAPPING\",\"RUNNING\",\"WAITING\",\"TERMINATING\"]}"));
    }

    public static MappingBuilder listStepsRequestWithClusterId(String clusterId) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_STEPS_OPERATION)
                .withRequestBody(equalToJson("{\"ClusterId\":\"" + clusterId + "\"}"));
    }

    public static RequestPatternBuilder listActiveClustersRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_LIST_CLUSTERS_OPERATION)
                .withRequestBody(equalToJson("{\"ClusterStates\":[" +
                        "\"STARTING\",\"BOOTSTRAPPING\",\"RUNNING\",\"WAITING\",\"TERMINATING\"]}"));
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningClusters(int numRunningClusters) {
        String clustersJson = IntStream.rangeClosed(1, numRunningClusters)
                .mapToObj(i -> "{" +
                        "\"Name\": \"sleeper-test-instance-test-cluster-" + i + "\"," +
                        "\"Id\": \"test-cluster-id-" + i + "\"," +
                        "\"Status\": {\"State\": \"RUNNING\"}" +
                        "}")
                .collect(Collectors.joining(",", "{\"Clusters\": [", "]}"));
        return aResponse().withStatus(200).withBody(clustersJson);
    }

    public static MappingBuilder terminateJobFlowsRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION)
                .willReturn(aResponse().withStatus(200));
    }

    public static MappingBuilder terminateJobFlowsRequestWithJobIdCount(int jobIdsCount) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION)
                .withRequestBody(matchingJsonPath("$.JobFlowIds.size()",
                        equalTo(jobIdsCount + "")))
                .willReturn(aResponse().withStatus(200));
    }


    public static RequestPatternBuilder terminateJobFlowsRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION);
    }

    public static RequestPatternBuilder terminateJobFlowsRequestedFor(String clusterId) {
        return terminateJobFlowsRequested()
                .withRequestBody(matchingJsonPath("$.JobFlowIds",
                        equalTo(clusterId)));
    }

    public static RequestPatternBuilder terminateJobFlowsRequestedWithJobIdsCount(int jobIdsCount) {
        return terminateJobFlowsRequested()
                .withRequestBody(matchingJsonPath("$.JobFlowIds.size()",
                        equalTo(jobIdsCount + "")));
    }
}
