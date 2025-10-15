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
import com.github.tomakehurst.wiremock.matching.StringValuePattern;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;

/**
 * Helper methods to mock an EMR cluster.
 */
public class WiremockEmrTestHelper {

    public static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_LIST_CLUSTERS_OPERATION = equalTo("ElasticMapReduce.ListClusters");

    private WiremockEmrTestHelper() {
    }

    /**
     * List active EMR clusters.
     *
     * @return Matching HTTP requests
     */
    public static MappingBuilder listActiveEmrClustersRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_CLUSTERS_OPERATION)
                .withRequestBody(equalToJson("{\"ClusterStates\":[" +
                        "\"STARTING\",\"BOOTSTRAPPING\",\"RUNNING\",\"WAITING\",\"TERMINATING\"]}"));
    }

    /**
     * Build an empty response.
     *
     * @return Matching HTTP requests
     */
    public static ResponseDefinitionBuilder aResponseWithNoClusters() {
        return aResponse().withStatus(200).withBody("{\"Clusters\": []}");
    }

}
