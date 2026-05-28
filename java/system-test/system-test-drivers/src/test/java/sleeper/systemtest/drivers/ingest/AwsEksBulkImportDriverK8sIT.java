/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.systemtest.drivers.ingest;

import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemoryTestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_NAMESPACE;

@InMemoryDslTest
@EnableKubernetesMockClient
public class AwsEksBulkImportDriverK8sIT {

    InstanceProperties instanceProperties = InMemoryTestInstance.createDslInstanceProperties();
    SystemTestContext context;
    KubernetesMockServer server;
    KubernetesClient client;

    @BeforeEach
    void setUp(SleeperDsl sleeper, SystemTestContext context) {
        this.context = context;
        instanceProperties.set(BULK_IMPORT_EKS_NAMESPACE, "test-namespace");
        sleeper.connectToInstanceNoTables(InMemoryTestInstance.usingSystemTestDefaultsWithProperties(instanceProperties));
    }

    @Test
    void shouldGetNoRunningPodsInSparkNamespace() {
        // Given
        server.expect().get()
                .withPath("/api/v1/namespaces/test-namespace/pods")
                .andReturn(200, new PodListBuilder().build())
                .always();

        // When / Then
        assertThat(driver().getRunningPods()).isEmpty();
    }

    @Test
    void shouldGetOneRunningPodInSparkNamespace() {
        // Given
        server.expect().get()
                .withPath("/api/v1/namespaces/test-namespace/pods")
                .andReturn(200, new PodListBuilder()
                        .addNewItem()
                        .and().build())
                .always();

        // When / Then
        assertThat(driver().getRunningPods()).hasSize(1);
    }

    private AwsEksBulkImportDriver driver() {
        return new AwsEksBulkImportDriver(context.instance(), context.sentIngestJobs(), null, properties -> client);
    }

}
