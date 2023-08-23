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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;


public class WiremockEMRTestHelper {
    private WiremockEMRTestHelper() {
    }

    public static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_LIST_CLUSTERS_OPERATION = matching("ElasticMapReduce.ListClusters");
    private static final StringValuePattern MATCHING_LIST_STEPS_OPERATION = matching("ElasticMapReduce.ListSteps");

    public static MappingBuilder listActiveClustersRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_CLUSTERS_OPERATION)
                .withRequestBody(equalToJson("{\"ClusterStates\":[" +
                        "\"STARTING\",\"BOOTSTRAPPING\",\"RUNNING\",\"WAITING\",\"TERMINATING\"]}"));
    }

    public static MappingBuilder listActiveApplicationsRequest() {
        return get("/applications");
    }

    public static MappingBuilder listJobsForApplicationsRequest(String applicationId) {
        return get(urlEqualTo("/applications/" + applicationId + "/jobruns"));
    }

    public static MappingBuilder deleteJobsForApplicationsRequest(String applicationId, String jobId) {
        return delete(urlEqualTo("/applications/" + applicationId + "/jobruns/" + jobId));
    }

    public static MappingBuilder  stopJobForApplicationsRequest(String applicationId) {
        return post(urlEqualTo("/applications/" + applicationId + "/stop"));
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

    public static RequestPatternBuilder listActiveApplicationRequested() {
        return getRequestedFor(urlEqualTo("/applications"));
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningClusters(int numRunningClusters) {
        StringBuilder clustersBody = new StringBuilder("{\"Clusters\": [");
        for (int i = 1; i <= numRunningClusters; i++) {
            clustersBody.append("{" +
                    "\"Name\": \"sleeper-test-instance-test-cluster-" + i + "\"," +
                    "\"Id\": \"test-cluster-id-" + i + "\"," +
                    "\"Status\": {\"State\": \"RUNNING\"}" +
                    "}");
            if (i != numRunningClusters) {
                clustersBody.append(",");
            }
        }
        clustersBody.append("]}");
        return aResponse().withStatus(200).withBody(clustersBody.toString());
    }

     public static ResponseDefinitionBuilder aResponseWithNumRunningApplications(int numRunningApplications) {
        return aResponseWithNumRunningApplications(numRunningApplications, false);
     }

    public static ResponseDefinitionBuilder aResponseWithNumRunningApplications(int numRunningApplications, boolean includeStoppedState) {
        StringBuilder applicationBody = new StringBuilder("{\"applications\": [");
        for (int i = 1; i <= numRunningApplications; i++) {
            String state = "RUNNING";

            if (i == 1 && includeStoppedState) {
                state = "STOPPED";
            }

            applicationBody.append("{" +
                    "\"name\": \"sleeper-test-instance-test-application-" + i + "\"," +
                    "\"id\": \"test-application-id-" + i + "\"," +
                    "\"state\": \"" + state + "\"" +
                    "}");
            if (i != numRunningApplications) {
                applicationBody.append(",");
            }
        }
        applicationBody.append("]}");
        return aResponse().withStatus(200).withBody(applicationBody.toString());
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningJobsOnApplication(int numRunningJobs) {
        StringBuilder jobRunBody = new StringBuilder("{\"jobRuns\": [");
        for (int i = 1; i <= numRunningJobs; i++) {
            jobRunBody.append("{" +
                    "\"applicationId\": \"sleeper-test-instance-test-application-" + i + "\"," +
                    "\"id\": \"test-job-run-id-" + i + "\"," +
                    "\"state\": \"RUNNING\"" +
                    "}");
            if (i != numRunningJobs) {
                jobRunBody.append(",");
            }
        }
        jobRunBody.append("]}");
        return aResponse().withStatus(200).withBody(jobRunBody.toString());
    }
}
