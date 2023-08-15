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

package sleeper.job.common;

import com.amazonaws.services.ecs.AmazonECS;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.job.common.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
public class CommonJobUtilsIT {
    @Nested
    @DisplayName("Count running and pending ECS tasks")
    class CountRunningAndPendingECSTasks {
        private AmazonECS ecsClient;

        @BeforeEach
        void setUp(WireMockRuntimeInfo runtimeInfo) {
            ecsClient = wiremockEcsClient(runtimeInfo);
        }

        @Test
        void shouldCountECSTasksWhenOneTaskIsRunning() {
            stubFor(listRunningTasks("test-cluster")
                    .willReturn(responseWithOneRunningTask()));
            assertThat(CommonJobUtils.getNumPendingAndRunningTasks("test-cluster", ecsClient))
                    .isEqualTo(1);
        }

        @Test
        void shouldCountECSTasksWhenNoTasksAreRunning() {
            stubFor(listRunningTasks("test-cluster")
                    .willReturn(responseWithNoRunningTasks()));
            assertThat(CommonJobUtils.getNumPendingAndRunningTasks("test-cluster", ecsClient))
                    .isEqualTo(0);
        }

        private ResponseDefinitionBuilder responseWithOneRunningTask() {
            return new ResponseDefinitionBuilder().withStatus(200)
                    .withBody("{" +
                            "\"taskArns\": [\"test-task\"]" +
                            "}");
        }

        private ResponseDefinitionBuilder responseWithNoRunningTasks() {
            return new ResponseDefinitionBuilder().withStatus(200)
                    .withBody("{" +
                            "\"taskArns\": []" +
                            "}");
        }

        private MappingBuilder listRunningTasks(String clusterName) {
            return post("/")
                    .withRequestBody(equalToJson("{" +
                            "\"cluster\":\"" + clusterName + "\"," +
                            "\"desiredStatus\":\"RUNNING\"" +
                            "}"
                    ));
        }
    }
}
