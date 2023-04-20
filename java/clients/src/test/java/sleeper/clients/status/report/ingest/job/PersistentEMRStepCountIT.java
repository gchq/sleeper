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

package sleeper.clients.status.report.ingest.job;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;

import java.util.Collections;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listActiveClustersRequest;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listStepsRequestWithClusterId;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;

@WireMockTest
class PersistentEMRStepCountIT {
    private final InstanceProperties properties = createTestInstanceProperties();

    @Test
    void shouldFindNoStepsForCluster(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": [{" +
                        "\"Name\":\"test-emr-cluster\"," +
                        "\"Id\":\"test-cluster-id\"" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("test-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": []}")));

        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo)).isEqualTo(Collections.emptyMap());
    }

    private Map<String, Integer> getStepCountByStatus(WireMockRuntimeInfo runtimeInfo) {
        return PersistentEMRStepCount.byStatus(wiremockEmrClient(runtimeInfo));
    }
}
