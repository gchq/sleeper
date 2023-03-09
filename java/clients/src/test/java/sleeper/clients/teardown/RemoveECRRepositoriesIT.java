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

package sleeper.clients.teardown;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClient;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;

import static com.amazonaws.regions.Regions.DEFAULT_REGION;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;

@WireMockTest
class RemoveECRRepositoriesIT {
    @Test
    @Disabled("TODO")
    void shouldRemoveRepositories(WireMockRuntimeInfo runtimeInfo) {
        AmazonECR ecrClient = AmazonECRClient.builder()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(runtimeInfo.getHttpsBaseUrl(), DEFAULT_REGION.getName()))
                .build();
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ECR_COMPACTION_REPO, "test-compaction-repo");

        // When
        RemoveECRRepositories.remove(ecrClient, properties);
    }
}
