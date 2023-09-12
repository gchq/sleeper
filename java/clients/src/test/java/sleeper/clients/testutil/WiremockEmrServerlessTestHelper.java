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
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class WiremockEmrServerlessTestHelper {

    private WiremockEmrServerlessTestHelper() {
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

    public static MappingBuilder stopJobForApplicationsRequest(String applicationId) {
        return post(urlEqualTo("/applications/" + applicationId + "/stop"));
    }

    public static RequestPatternBuilder listActiveApplicationRequested() {
        return getRequestedFor(urlEqualTo("/applications"));
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningApplications(int numRunningApplications) {
        List<ApplicationState> states = new ArrayList<>();
        for (int i = 0; i < numRunningApplications; i++) {
            states.add(ApplicationState.STARTED);
        }
        return aResponseWithNumRunningApplications(states);
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningApplications(List<ApplicationState> states) {
        StringBuilder applicationBody = new StringBuilder("{\"applications\": [");
        for (int i = 1; i < states.size(); i++) {
            applicationBody.append("{" +
                    "\"name\": \"sleeper-test-instance-test-application-" + i + "\"," +
                    "\"id\": \"test-application-id-" + i + "\"," +
                    "\"state\": \"" + states.get(i).toString() + "\"" +
                    "}");
            if (i != states.size()) {
                applicationBody.append(",");
            }
        }
        applicationBody.append("]}");
        return aResponse().withStatus(200).withBody(applicationBody.toString());
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningJobsOnApplication(int numRunningJobs) {
        return aResponseWithNumRunningJobsOnApplication(numRunningJobs, false);
    }

    public static ResponseDefinitionBuilder aResponseWithNumRunningJobsOnApplication(int numRunningJobs, boolean includeSuccessState) {
        StringBuilder jobRunBody = new StringBuilder("{\"jobRuns\": [");
        for (int i = 1; i <= numRunningJobs; i++) {

            String state = "RUNNING";

            if (i == 1 && includeSuccessState) {
                state = "SUCCESS";
            }
            jobRunBody.append("{" +
                    "\"applicationId\": \"sleeper-test-instance-test-application-" + i + "\"," +
                    "\"id\": \"test-job-run-id-" + i + "\"," +
                    "\"state\": \"" + state + "\"" +
                    "}");
            if (i != numRunningJobs) {
                jobRunBody.append(",");
            }
        }
        jobRunBody.append("]}");
        return aResponse().withStatus(200).withBody(jobRunBody.toString());
    }
}
