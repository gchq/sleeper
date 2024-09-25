/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.StaticRateLimit;

import java.util.Collections;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveEmrClustersRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listStepsRequestWithClusterId;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;

@WireMockTest
class PersistentEMRStepCountIT {
    private final InstanceProperties properties = createTestInstanceProperties();

    @Test
    void shouldFindNoStepsForCluster(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveEmrClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": [{" +
                        "\"Name\":\"test-emr-cluster\"," +
                        "\"Id\":\"test-cluster-id\"" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("test-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": []}")));

        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Collections.emptyMap());
    }

    @Test
    void shouldFindNoStepsWhenClusterHasTerminated(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveEmrClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": []}")));

        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Collections.emptyMap());
    }

    @Test
    void shouldFindNoPersistentClusterDeployed(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.unset(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME);

        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Collections.emptyMap());
    }

    @Test
    void shouldFindOnePendingStepForCluster(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveEmrClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": [{" +
                        "\"Name\":\"test-emr-cluster\"," +
                        "\"Id\":\"test-cluster-id\"" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("test-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": [{" +
                        "\"Status\":{\"State\":\"PENDING\"}" +
                        "}]}")));

        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Map.of("PENDING", 1));
    }

    @Test
    void shouldFindOnePendingStepForClusterWhenOtherClustersArePresent(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveEmrClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": [{" +
                        "\"Name\":\"test-emr-cluster\"," +
                        "\"Id\":\"test-cluster-id\"" +
                        "},{" +
                        "\"Name\":\"other-emr-cluster\"," +
                        "\"Id\":\"other-cluster-id\"" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("test-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": [{" +
                        "\"Status\":{\"State\":\"PENDING\"}" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("other-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": [{" +
                        "\"Status\":{\"State\":\"PENDING\"}" +
                        "}]}")));

        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Map.of("PENDING", 1));
    }

    @Test
    void shouldFindStepsWithDifferentStatesForCluster(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveEmrClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": [{" +
                        "\"Name\":\"test-emr-cluster\"," +
                        "\"Id\":\"test-cluster-id\"" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("test-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": [{" +
                        "\"Status\":{\"State\":\"PENDING\"}" +
                        "},{" +
                        "\"Status\":{\"State\":\"RUNNING\"}" +
                        "},{" +
                        "\"Status\":{\"State\":\"COMPLETED\"}" +
                        "}]}")));
        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Map.of(
                        "PENDING", 1,
                        "RUNNING", 1,
                        "COMPLETED", 1));
    }

    @Test
    void shouldCountMultipleStepsWithSameStateForCluster(WireMockRuntimeInfo runtimeInfo) {
        // Given
        properties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, "test-emr-cluster");
        stubFor(listActiveEmrClustersRequest().willReturn(aResponse().withStatus(200)
                .withBody("{\"Clusters\": [{" +
                        "\"Name\":\"test-emr-cluster\"," +
                        "\"Id\":\"test-cluster-id\"" +
                        "}]}")));
        stubFor(listStepsRequestWithClusterId("test-cluster-id").willReturn(aResponse().withStatus(200)
                .withBody("{\"Steps\": [{" +
                        "\"Status\":{\"State\":\"PENDING\"}" +
                        "},{" +
                        "\"Status\":{\"State\":\"PENDING\"}" +
                        "}]}")));
        // When / Then
        assertThat(getStepCountByStatus(runtimeInfo))
                .isEqualTo(Map.of("PENDING", 2));
    }

    private Map<String, Integer> getStepCountByStatus(WireMockRuntimeInfo runtimeInfo) {
        return PersistentEMRStepCount.byStatus(properties, wiremockEmrClient(runtimeInfo), StaticRateLimit.none());
    }
}
